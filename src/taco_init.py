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
    DeviceStatus,
    ZoneInfo,
    NETWORK_DEVICE_INDEX
)
from .ble_data_update_coordinator import BleDataUpdateCoordinator


_LOGGER = logging.getLogger(__name__)

# The password login state (td_status_char), see DeviceStatus.
_DEVICE_STATUS_UUID = "38f63145-02b6-403c-810c-7e1253f474eb"

# Selects which networked controller indexed characteristics address.
_NETWORK_DEVICE_INDEX_UUID = "1b423159-e0eb-4d9e-a86b-dcabcc3565b9"


class TacoNotAuthenticated(Exception):
    """The device did not accept the password, or is locked out."""


async def _read_device_status(ble_coordinator: BleDataUpdateCoordinator) -> DeviceStatus:
    """Read the login state, giving the device a moment to process the password."""

    for _ in range(3):
        status = await ble_coordinator.read(_DEVICE_STATUS_UUID)
        if status.authenticated or status.locked or status.long_locked:
            return status
        await asyncio.sleep(0.2)
    return status


async def _authenticate(
    password: MaskedString, ble_coordinator: BleDataUpdateCoordinator
) -> None:
    """Write the password, then check the device actually accepted it.

    The password write itself succeeds even when the password is wrong,
    the result only shows up in the status flags.
    """

    await ble_coordinator.write([WriteRequest(PROVIDE_PASSWORD, extra=password)])
    status = await _read_device_status(ble_coordinator)
    if status.locked or status.long_locked:
        raise TacoNotAuthenticated(
            f"the device is locked out after too many password attempts ({status})"
        )
    if not status.authenticated:
        raise TacoNotAuthenticated(f"the device did not accept the password ({status})")


async def _select_device(ble_coordinator: BleDataUpdateCoordinator) -> None:
    """Select this controller for indexed commands such as forcing zones.

    The app writes the device index right before every indexed command,
    after logging in, rather than relying on an earlier selection.
    """

    network_device_index = await ble_coordinator.read(_NETWORK_DEVICE_INDEX_UUID)
    await ble_coordinator.write(
        [WriteRequest(PING_NETWORK_DEVICE_INDEX, extra=network_device_index)]
    )


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

    # Only report the login state for now, rather than failing setup,
    # so sensors keep working while the status flags are confirmed.
    try:
        status = await _read_device_status(ble_coordinator)
    except Exception:  # Reading the status is diagnostic only.
        _LOGGER.warning("Could not read the password status", exc_info=True)
        return
    if status.authenticated:
        _LOGGER.info("Password accepted (%s)", status)
    else:
        _LOGGER.error(
            "Password not accepted (%s), forcing zones on will fail. "
            "Check the password printed inside the green cover.",
            status,
        )


async def send_initial_write_requests(runtime_data: TacoRuntimeData):
    """Starts communication with the taco, validating passwords and connections."""

    await _validate_ping(runtime_data.ble_coordinator)
    await _validate_password(runtime_data.password, runtime_data.ble_coordinator)

def _create_write_requests(runtime_data: TacoRuntimeData) -> list[WriteRequest]:
    """The write actions that should take place upon a successful loop.

    The Taco has no force off command (the app never sends a force with
    no zones). So when no zone is forced nothing is sent, and the last
    force simply expires after 5 minutes.
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
    for action in actions:
        if action.action == PROVIDE_PASSWORD:
            await _authenticate(action.extra, ble_coordinator)
            continue
        if action.action == FORCE_ZONE_ON:
            await _select_device(ble_coordinator)
        await ble_coordinator.write([action])


_PREVIOUS_ACTIONS_KEY = "previous_actions"
_PREVIOUS_WRITE_TIME_KEY = "previous_write_time"
_RETRY_AT_KEY = "retry_at"
_RETRY_DELAY_KEY = "retry_delay"
_FAILURES_KEY = "failures"

# The Taco times out after 5 minutes, so resend just a bit before.
_KEEP_ALIVE_INTERVAL = timedelta(minutes=4)

# After a failed write, wait this long before trying again, doubling each time.
_MIN_RETRY_DELAY = timedelta(seconds=5)
_MAX_RETRY_DELAY = timedelta(minutes=1)

# Give up after this many failed writes in a row (about 75 seconds of retries),
# and turn the force switches off so they show the zones are not forced.
_MAX_WRITE_ATTEMPTS = 5


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
    except TacoNotAuthenticated as err:
        # Don't retry, every retry is another password attempt.
        _give_up(state, runtime_data, f"{err}, check the password")
        return
    except Exception as err:  # Retry later, rather than every second.
        failures = state.get(_FAILURES_KEY, 0) + 1
        if failures >= _MAX_WRITE_ATTEMPTS:
            _give_up(state, runtime_data, f"{err} (after {failures} attempts)")
            return

        state[_FAILURES_KEY] = failures
        retry_delay = min(
            state.get(_RETRY_DELAY_KEY, _MIN_RETRY_DELAY / 2) * 2, _MAX_RETRY_DELAY
        )
        state[_RETRY_DELAY_KEY] = retry_delay
        state[_RETRY_AT_KEY] = now + retry_delay
        _LOGGER.warning(
            "Failed to force zones on for device %s (attempt %s of %s), retrying in %s: %s",
            runtime_data.address,
            failures,
            _MAX_WRITE_ATTEMPTS,
            retry_delay,
            err,
        )
        return

    state.pop(_FAILURES_KEY, None)
    state.pop(_RETRY_DELAY_KEY, None)
    state.pop(_RETRY_AT_KEY, None)
    state[_PREVIOUS_ACTIONS_KEY] = actions
    state[_PREVIOUS_WRITE_TIME_KEY] = now


def _give_up(state: dict, runtime_data: TacoRuntimeData, reason: str) -> None:
    """Stop forcing, and turn the switches off so they show the zones are not forced."""

    _LOGGER.error(
        "Giving up forcing zones on for device %s, turning the force switches off: %s",
        runtime_data.address,
        reason,
    )
    runtime_data.force_zone_on[:] = [False] * len(runtime_data.force_zone_on)
    state.clear()
    runtime_data.update_coordinator.async_update_listeners()


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
