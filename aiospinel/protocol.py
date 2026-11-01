from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator, Callable
import dataclasses
import logging
from typing import Literal, TypedDict, Union, cast, overload

from typing_extensions import Unpack

from aiospinel.types import (
    CommandID,
    HDLCSpecial,
    PackedUInt21,
    PropertyID,
    ResetReason,
    Status,
)

_LOGGER = logging.getLogger(__name__)

PropertyKey = Union[PropertyID, int]

COMMAND_TIMEOUT = 2
RESET_TIMEOUT = 2


class SendOptions(TypedDict, total=False):
    retries: int
    timeout: float
    retry_delay: float
    tid: int | None


class PropertyStatusError(Exception):
    """The device replied to a property request with a status instead of a value."""

    def __init__(self, property_id: PropertyKey, status: Status | int) -> None:
        super().__init__(f"Property {property_id!r} request failed: {status!r}")
        self.property_id = property_id
        self.status = status


def crc16_kermit(data: bytes | bytearray) -> int:
    """HDLC-Lite CRC-16 (reflected poly 0x1021, init 0xFFFF, final xor 0xFFFF)."""
    crc = 0xFFFF

    for byte in data:
        crc ^= byte

        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0x8408
            else:
                crc >>= 1

    return crc ^ 0xFFFF


class SerialProtocol(asyncio.Protocol):
    """Base class for packet-parsing serial protocol implementations."""

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._transport: asyncio.Transport | None = None

        self._connected_event = asyncio.Event()
        self._disconnected_event = asyncio.Event()
        self._disconnected_event.set()

    async def wait_until_connected(self) -> None:
        """Wait for the protocol's transport to be connected."""
        await self._connected_event.wait()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        _LOGGER.debug("Connection made: %s", transport)

        self._transport = cast("asyncio.Transport", transport)
        self._disconnected_event.clear()
        self._connected_event.set()

    def connection_lost(self, exc: BaseException | None) -> None:
        _LOGGER.debug("Connection lost: %r", exc)
        self._connected_event.clear()
        self._disconnected_event.set()
        self._transport = None

    def data_received(self, data: bytes) -> None:
        self._buffer += data

    def close(self) -> None:
        self._buffer.clear()

        if self._transport is not None:
            self._transport.close()

    async def wait_until_closed(self) -> None:
        _LOGGER.debug("Waiting for serial port to close")
        await self._disconnected_event.wait()

    async def disconnect(self) -> None:
        self.close()
        await self.wait_until_closed()


@dataclasses.dataclass(frozen=True)
class HDLCLiteFrame:
    data: bytes

    def serialize(self) -> bytes:
        payload = self.data + crc16_kermit(self.data).to_bytes(2, "little")
        encoded = bytearray()

        for byte in payload:
            if byte in (
                HDLCSpecial.FLAG,
                HDLCSpecial.ESCAPE,
                HDLCSpecial.XON,
                HDLCSpecial.XOFF,
                HDLCSpecial.VENDOR,
            ):
                encoded.append(HDLCSpecial.ESCAPE)
                byte ^= 0x20

            encoded.append(byte)

        return bytes([HDLCSpecial.FLAG]) + bytes(encoded) + bytes([HDLCSpecial.FLAG])

    @classmethod
    def from_bytes(cls, data: bytes | bytearray) -> HDLCLiteFrame:
        unescaped = bytearray()
        unescaping = False

        for byte in data:
            if unescaping:
                byte ^= 0x20

                if byte not in (
                    HDLCSpecial.FLAG,
                    HDLCSpecial.ESCAPE,
                    HDLCSpecial.XON,
                    HDLCSpecial.XOFF,
                    HDLCSpecial.VENDOR,
                ):
                    raise ValueError(f"Invalid unescaped byte: 0x{byte:02X}")

                unescaping = False
            elif byte == HDLCSpecial.ESCAPE:
                unescaping = True
                continue
            elif byte == HDLCSpecial.FLAG:
                continue

            unescaped.append(byte)

        data = unescaped[:-2]
        crc = unescaped[-2:]
        computed_crc = crc16_kermit(data).to_bytes(2, "little")

        if computed_crc != crc:
            raise ValueError(f"Invalid CRC-16: expected {crc!r}, got {computed_crc!r}")

        return cls(data=bytes(data))


@dataclasses.dataclass(frozen=True)
class SpinelHeader:
    # Header byte layout (MSB first): flag (2 bits) | network_link_id (2) | tid (4)
    transaction_id: int | None
    network_link_id: int
    flag: int

    def serialize(self) -> bytes:
        assert self.transaction_id is not None
        return bytes(
            [(self.flag << 6) | (self.network_link_id << 4) | self.transaction_id]
        )

    @classmethod
    def deserialize(cls, data: bytes) -> tuple[SpinelHeader, bytes]:
        byte = data[0]

        return cls(
            transaction_id=byte & 0b1111,
            network_link_id=(byte >> 4) & 0b11,
            flag=(byte >> 6) & 0b11,
        ), data[1:]


@dataclasses.dataclass(frozen=True)
class SpinelFrame:
    header: SpinelHeader
    command_id: CommandID
    data: bytes

    @classmethod
    def from_bytes(cls, data: bytes) -> SpinelFrame:
        orig_data = data
        header, data = SpinelHeader.deserialize(data)

        if header.flag != 0b10:
            raise ValueError(f"Spinel header flag is invalid in frame: {orig_data!r}")

        raw_command_id, data = CommandID.deserialize(data)

        return cls(header=header, command_id=CommandID(raw_command_id), data=data)

    def serialize(self) -> bytes:
        return self.header.serialize() + self.command_id.serialize() + self.data


class SpinelProtocol(SerialProtocol):
    _buffer: bytearray

    def __init__(self) -> None:
        super().__init__()

        # TID 0 is reserved for no-response frames, leaving 1-15 for requests
        self._free_tids: asyncio.Queue[int] = asyncio.Queue()
        for tid in range(1, 16):
            self._free_tids.put_nowait(tid)

        self._pending_frames: dict[int, asyncio.Future[SpinelFrame]] = {}
        self._property_listeners: defaultdict[
            PropertyKey, list[Callable[[bytes], None]]
        ] = defaultdict(list)

    def send_data(self, data: bytes) -> None:
        assert self._transport is not None
        _LOGGER.debug("Sending data %s", data)
        self._transport.write(data)

    def data_received(self, data: bytes) -> None:
        super().data_received(data)

        while self._buffer:
            chunk, flag, self._buffer = self._buffer.partition(
                bytes([HDLCSpecial.FLAG])
            )

            # If the flag isn't found, we're done
            if not flag:
                self._buffer = chunk
                break

            # Sometimes the flag can be repeated multiple times
            if not chunk:
                continue

            # Decode the HDLC frame
            try:
                hdlc_frame = HDLCLiteFrame.from_bytes(chunk)
            except ValueError:
                _LOGGER.debug("Failed to decode HDLC chunk %r", chunk)
                continue

            _LOGGER.debug("Decoded HDLC frame: %r", hdlc_frame)

            # And finally the Spinel frame
            try:
                spinel_frame = SpinelFrame.from_bytes(hdlc_frame.data)
            except ValueError as e:
                _LOGGER.debug("Failed to decode Spinel frame: %r", e)
                continue

            self.frame_received(spinel_frame)

    def frame_received(self, frame: SpinelFrame) -> None:
        _LOGGER.debug("Parsed frame %r", frame)

        if frame.header.transaction_id in self._pending_frames:
            fut = self._pending_frames[frame.header.transaction_id]

            if fut.done():
                _LOGGER.debug(
                    "Ignoring duplicate response for TID %d",
                    frame.header.transaction_id,
                )
            else:
                fut.set_result(frame)

        if frame.command_id == CommandID.PROP_VALUE_IS:
            raw_prop_id, data = PackedUInt21.deserialize(frame.data)
            # A known core property resolves to its enum member; an unknown (e.g.
            # vendor) id is dispatched by its raw integer value.
            try:
                prop_id: PropertyKey = PropertyID(raw_prop_id)
            except ValueError:
                prop_id = raw_prop_id

            for listener in self._property_listeners[prop_id]:
                try:
                    listener(data)
                except Exception:
                    _LOGGER.warning(
                        "Error calling property listener for %r: %r",
                        prop_id,
                        listener,
                        exc_info=True,
                    )

    @overload
    async def send_frame(
        self,
        frame: SpinelFrame,
        *,
        wait_response: Literal[False],
        retries: int = ...,
        timeout: float = ...,
        retry_delay: float = ...,
        tid: int | None = ...,
    ) -> None: ...

    @overload
    async def send_frame(
        self,
        frame: SpinelFrame,
        *,
        wait_response: Literal[True],
        retries: int = ...,
        timeout: float = ...,
        retry_delay: float = ...,
        tid: int | None = ...,
    ) -> SpinelFrame: ...

    async def send_frame(
        self,
        frame: SpinelFrame,
        *,
        wait_response: bool = True,
        retries: int = 2,
        timeout: float = COMMAND_TIMEOUT,
        retry_delay: float = 0.1,
        tid: int | None = None,
    ) -> SpinelFrame | None:
        needs_tid = tid is None and wait_response

        if needs_tid:
            tid = await self._free_tids.get()
        elif tid is None:
            tid = 0

        # Replace the transaction ID
        new_header = dataclasses.replace(frame.header, transaction_id=tid)
        new_frame = dataclasses.replace(frame, header=new_header)

        if not wait_response:
            _LOGGER.debug("Sending frame %r", new_frame)
            self.send_data(HDLCLiteFrame(data=new_frame.serialize()).serialize())
            return None

        if tid == 0:
            raise ValueError("Cannot wait for response on TID=0 frames")

        future = asyncio.get_running_loop().create_future()
        self._pending_frames[tid] = future

        try:
            for attempt in range(retries + 1):
                _LOGGER.debug("Sending frame %r", new_frame)
                self.send_data(HDLCLiteFrame(data=new_frame.serialize()).serialize())

                try:
                    async with asyncio.timeout(timeout):
                        return await asyncio.shield(future)
                except TimeoutError:
                    _LOGGER.debug(
                        "Failed to send %s, trying again in %0.2fs (attempt %s of %s)",
                        frame,
                        retry_delay,
                        attempt + 1,
                        retries + 1,
                    )

                    if attempt >= retries:
                        raise

                    await asyncio.sleep(retry_delay)
        finally:
            self._pending_frames.pop(tid, None)
            if needs_tid:
                self._free_tids.put_nowait(tid)

        raise AssertionError("Unreachable")

    @overload
    async def send_command(
        self,
        command_id: CommandID,
        data: bytes,
        *,
        wait_response: Literal[False],
        **kwargs: Unpack[SendOptions],
    ) -> None: ...

    @overload
    async def send_command(
        self,
        command_id: CommandID,
        data: bytes,
        *,
        wait_response: Literal[True] = ...,
        **kwargs: Unpack[SendOptions],
    ) -> SpinelFrame: ...

    async def send_command(
        self,
        command_id: CommandID,
        data: bytes,
        *,
        wait_response: bool = True,
        **kwargs: Unpack[SendOptions],
    ) -> SpinelFrame | None:
        frame = SpinelFrame(
            header=SpinelHeader(
                flag=0b10,
                network_link_id=0,
                transaction_id=None,
            ),
            command_id=command_id,
            data=data,
        )

        if not wait_response:
            await self.send_frame(frame, wait_response=False, **kwargs)
            return None

        return await self.send_frame(frame, wait_response=True, **kwargs)

    async def get_property(
        self, property_id: PropertyKey, **kwargs: Unpack[SendOptions]
    ) -> bytes:
        return await self._property_command(
            CommandID.PROP_VALUE_GET, property_id, b"", **kwargs
        )

    async def set_property(
        self, property_id: PropertyKey, value: bytes, **kwargs: Unpack[SendOptions]
    ) -> bytes | None:
        try:
            return await self._property_command(
                CommandID.PROP_VALUE_SET, property_id, value, **kwargs
            )
        except PropertyStatusError as exc:
            # A successful SET of a property without a getter is answered with `OK`
            if exc.status != Status.OK:
                raise

            return None

    async def _property_command(
        self,
        command_id: CommandID,
        property_id: PropertyKey,
        data: bytes,
        **kwargs: Unpack[SendOptions],
    ) -> bytes:
        rsp = await self.send_command(
            command_id,
            PackedUInt21(property_id).serialize() + data,
            **kwargs,
        )
        rsp_prop_id, value = PackedUInt21.deserialize(rsp.data)

        if rsp_prop_id == property_id:
            return value

        # Unknown properties are answered with `LAST_STATUS` (e.g. `PROP_NOT_FOUND`)
        if rsp_prop_id == PropertyID.LAST_STATUS:
            raw_status, _ = PackedUInt21.deserialize(value)

            # Vendor and stack-native statuses have no `Status` member
            try:
                status: Status | int = Status(raw_status)
            except ValueError:
                status = raw_status

            raise PropertyStatusError(property_id, status)

        raise ValueError(f"Unexpected response for property {property_id!r}: {rsp!r}")

    def add_property_listener(
        self, property_id: PropertyKey, callback: Callable[[bytes], None]
    ) -> None:
        self._property_listeners[property_id].append(callback)

    def remove_property_listener(
        self, property_id: PropertyKey, callback: Callable[[bytes], None]
    ) -> None:
        self._property_listeners[property_id].remove(callback)

    async def iter_property_changes(
        self, property_id: PropertyKey
    ) -> AsyncIterator[bytes]:
        queue: asyncio.Queue[bytes] = asyncio.Queue()

        try:
            self.add_property_listener(property_id, queue.put_nowait)

            while True:
                item = await queue.get()
                yield item
        finally:
            self.remove_property_listener(property_id, queue.put_nowait)

    async def wait_for_property(self, property_id: PropertyKey, value: bytes) -> None:
        async for changed_value in self.iter_property_changes(property_id):
            if changed_value == value:
                return

    async def probe(self) -> str:
        await self.reset(ResetReason.STACK)

        rsp = await self.send_command(
            CommandID.PROP_VALUE_GET,
            PropertyID.NCP_VERSION.serialize(),
        )

        prop_id, version_string = PropertyID.deserialize(rsp.data)
        assert prop_id == PropertyID.NCP_VERSION

        # SL-OPENTHREAD/2.2.2.0_GitHub-91fa1f455; EFR32; Mar 14 2023 16:03:40
        version = version_string.rstrip(b"\x00").decode("ascii")

        # We strip off the date code to get something reasonably stable
        short_version, _ = version.split(";", 1)

        return short_version

    async def enter_bootloader(self) -> None:
        await self.reset(ResetReason.BOOTLOADER)

    async def reset(self, reset_type: ResetReason) -> None:
        await self.send_command(
            CommandID.RESET,
            reset_type.serialize(),
            wait_response=False,
            tid=0,  # Reset uses TID=0
        )

        if reset_type == ResetReason.BOOTLOADER:
            return

        try:
            async with asyncio.timeout(RESET_TIMEOUT):
                await self.wait_for_property(
                    PropertyID.LAST_STATUS, Status.RESET_POWER_ON.serialize()
                )
        except TimeoutError:
            # OTBR itself uses this logic, we match it
            _LOGGER.debug("Device did not respond to reset, continuing")
