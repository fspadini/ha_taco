"""Methods used inside of __init__.py for setting up the taco."""

import asyncio
from datetime import datetime, timedelta
import logging

from homeassistant.helpers.event import async_track_time_interval
from homeassistant.exceptions import (ConfigEntryAuthFailed, ConfigEntryNotReady)
from homeassistant.core import HomeAssistant

from .taco_gatt_write_transform import (
    PROVIDE_PASSWORD,
    PING_NETWORK_DEVICE_INDEX,
    WriteRequest,
    FORCE_ZONE_ON,
    MaskedString
)
from .taco_config_entry import TacoRuntimeData
from .taco_gatt_read_transform import (
    ZoneInfo,
    NETWORK_DEVICE_INDEX
)
from .ble_data_update_coordinator import BleDataUpdateCoordinator


_LOGGER = logging.getLogger(__name__)


async def _validate_ping(ble_coordinator: BleDataUpdateCoordinator):
    """Reads then writes the mac address."""
    try:
        results = await ble_coordinator.poll()
        network_device_index = results.get(NETWORK_DEVICE_INDEX)
        if network_device_index is None:
            raise Exception("Could not get network device index, is the device online and connected?")

        # In theory we should be able to make this write request prior to sending the password
        # Since it is just a ping. However, if a bug report comes in then we may need to remove the write.
        await ble_coordinator.write([WriteRequest(PING_NETWORK_DEVICE_INDEX, extra=network_device_index)])
    except Exception as err:
        raise ConfigEntryNotReady(err) from err


async def _validate_password(password: MaskedString | None, ble_coordinator: BleDataUpdateCoordinator):
    """Checks the password is legal and actually correct for this particular device."""

    if not password or not password.value:
        return

    if len(password.value) > 20:
        raise ConfigEntryAuthFailed(
            "Cannot have a Taco password more than 20 characters."
        )

    try:
        await ble_coordinator.write([WriteRequest(PROVIDE_PASSWORD, extra=password)])
    except Exception as err:
        raise ConfigEntryAuthFailed(err) from err


async def send_initial_write_requests(runtime_data: TacoRuntimeData):
    """Starts communication with the taco, validating passwords and connections."""

    await _validate_ping(runtime_data.ble_coordinator)
    await _validate_password(runtime_data.password, runtime_data.ble_coordinator)

def _create_write_requests(runtime_data: TacoRuntimeData) -> list[WriteRequest]:
    """The write actions that should take place upon a successful loop.

    The Taco has no force off command and rejects a force with no zones
    (GATT error 252, write request rejected). So when no zone is forced
    nothing is sent, and the last force simply expires after 5 minutes.
    """

    if not any(runtime_data.force_zone_on):
        return []

    zone_info = ZoneInfo(
        zone1=runtime_data.force_zone_on[0],
        zone2=runtime_data.force_zone_on[1],
        zone3=runtime_data.force_zone_on[2],
        zone4=runtime_data.force_zone_on[3],
        zone5=runtime_data.force_zone_on[4],
        zone6=runtime_data.force_zone_on[5],
    )

    return [
        WriteRequest(PROVIDE_PASSWORD, extra=runtime_data.password),
        WriteRequest(FORCE_ZONE_ON, extra=zone_info),
    ]


async def _send_write_requests(
    actions: list[WriteRequest], ble_coordinator: BleDataUpdateCoordinator
) -> None:
    _LOGGER.info(
        "Sending out write requests (%s): %s",
        len(actions),
        actions,
    )
    await ble_coordinator.write(actions)


_PREVIOUS_ACTIONS_KEY = "previous_actions"
_PREVIOUS_WRITE_TIME_KEY = "previous_write_time"
_RETRY_AT_KEY = "retry_at"
_RETRY_DELAY_KEY = "retry_delay"

# The Taco times out after 5 minutes, so resend just a bit before.
_KEEP_ALIVE_INTERVAL = timedelta(minutes=4)

# After a failed write, wait this long before trying again, doubling each time.
_MIN_RETRY_DELAY = timedelta(seconds=5)
_MAX_RETRY_DELAY = timedelta(minutes=1)


async def _loop(state: dict, runtime_data: TacoRuntimeData):
    """The actual loop called every second."""

    # The Taco is pretty defensive and will timeout after
    # 5 minutes. So we need to repeatedly send it the same commands
    # over and over again to keep it awake and acting like we want.

    actions = _create_write_requests(runtime_data)
    if not actions:
        state.clear()
        return

    now = datetime.now()
    if now < state.get(_RETRY_AT_KEY, now):
        return

    previous_write_time = state.get(_PREVIOUS_WRITE_TIME_KEY)
    if (
        state.get(_PREVIOUS_ACTIONS_KEY) == actions
        and previous_write_time is not None
        and now - previous_write_time <= _KEEP_ALIVE_INTERVAL
    ):
        return

    try:
        await _send_write_requests(actions, runtime_data.ble_coordinator)
    except Exception as err:  # Retry later, rather than every second.
        retry_delay = min(
            state.get(_RETRY_DELAY_KEY, _MIN_RETRY_DELAY / 2) * 2, _MAX_RETRY_DELAY
        )
        state[_RETRY_DELAY_KEY] = retry_delay
        state[_RETRY_AT_KEY] = now + retry_delay
        _LOGGER.warning(
            "Failed to force zones on for device %s, retrying in %s: %s",
            runtime_data.address,
            retry_delay,
            err,
        )
        return

    state.pop(_RETRY_DELAY_KEY, None)
    state.pop(_RETRY_AT_KEY, None)
    state[_PREVIOUS_ACTIONS_KEY] = actions
    state[_PREVIOUS_WRITE_TIME_KEY] = now


def _make_tick(state: dict, runtime_data: TacoRuntimeData):
    """Wrap the loop so a slow write (eg, a reconnect) is never run twice at once."""

    lock = asyncio.Lock()

    async def _tick(_time):
        if lock.locked():
            return
        async with lock:
            await _loop(state, runtime_data)

    return _tick


async def setup_write_loop(hass: HomeAssistant, runtime_data: TacoRuntimeData):
    """Sets up the loop(s) that write data back to the device."""

    return async_track_time_interval(
        hass,
        _make_tick({}, runtime_data),
        # Don't change this, we need it to be relatively fast
        # If you want to adjust things, adjust inside of _loop.
        timedelta(seconds=1),
    )
