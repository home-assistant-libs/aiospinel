from __future__ import annotations

import asyncio
import logging
from unittest.mock import MagicMock, call

import pytest

from aiospinel.protocol import (
    HDLCLiteFrame,
    PropertyStatusError,
    SpinelFrame,
    SpinelHeader,
    SpinelProtocol,
    crc16_kermit,
)
from aiospinel.types import CommandID, HDLCSpecial, PackedUInt21, PropertyID, Status
from tests.common import PairedTransport

_LOGGER = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "data, crc",
    [
        (b"", 0x0000),
        (b"foobar", 0x147B),
        (b"\xfa\x9b\x51\xb9\xf2\x53\xe3\xbd", 0x6782),
    ],
)
def test_hdlc_lite_crc(data, crc):
    assert crc16_kermit(data) == crc


@pytest.mark.parametrize(
    "encoded, decoded",
    [
        (bytes.fromhex("7e810243d3d37e"), bytes.fromhex("810243")),
        (bytes.fromhex("7e8103367d5e7d5d6af97e"), bytes.fromhex("8103367e7d")),
        (bytes.fromhex("7e810365010b287e"), bytes.fromhex("81036501")),
        (bytes.fromhex("7e8103862a01547d5e7e"), bytes.fromhex("8103862a01")),
        (
            bytes.fromhex(
                "7e8106024f50454e5448524541442f366666316163302d64697274793b204546523332"
                "3b2044656320323320323032322031383a30383a303000fa8c7e"
            ),
            bytes.fromhex(
                "8106024f50454e5448524541442f366666316163302d64697274793b2045465233323b"
                "2044656320323320323032322031383a30383a303000"
            ),
        ),
    ],
)
def test_hdlc_lite_encoding_decoding(encoded, decoded):
    assert HDLCLiteFrame(data=decoded).serialize() == encoded
    assert HDLCLiteFrame.from_bytes(encoded).data == decoded


@pytest.mark.parametrize(
    "encoded, decoded",
    [
        (
            bytes.fromhex(
                "8106024f50454e5448524541442f366666316163302d64697274793b2045465233323b"
                "2044656320323320323032322031383a30383a303000"
            ),
            SpinelFrame(
                header=SpinelHeader(
                    transaction_id=1,
                    network_link_id=0,
                    flag=0b10,
                ),
                command_id=CommandID.PROP_VALUE_IS,
                data=b"\x02OPENTHREAD/6ff1ac0-dirty; EFR32; Dec 23 2022 18:08:00\x00",
            ),
        ),
    ],
)
def test_spinel_parsing(encoded, decoded):
    assert SpinelFrame.from_bytes(encoded) == decoded
    assert decoded.serialize() == encoded


class Conversation(asyncio.Protocol):
    """A helper to script a conversation with a protocol."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.transport: PairedTransport | None = None
        self._reader = asyncio.StreamReader(loop=loop)

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        assert isinstance(transport, PairedTransport)
        self.transport = transport

    def data_received(self, data: bytes) -> None:
        self._reader.feed_data(data)

    def connection_lost(self, exc: Exception | None) -> None:
        self._reader.feed_eof()

    async def send(self, data: bytes) -> None:
        assert self.transport is not None
        self.transport.write(data)

    async def send_frame(self, frame: SpinelFrame) -> None:
        await self.send(HDLCLiteFrame(data=frame.serialize()).serialize())

    async def receive(self) -> SpinelFrame:
        """Read the next Spinel frame the client sent, skipping HDLC flag delimiters."""
        while True:
            chunk = (await self._reader.readuntil(bytes([HDLCSpecial.FLAG])))[:-1]
            if chunk:
                return SpinelFrame.from_bytes(HDLCLiteFrame.from_bytes(chunk).data)


async def create_spinel_test_pair() -> tuple[SpinelProtocol, Conversation]:
    """Creates a connected pair of a client protocol and a conversation helper."""
    loop = asyncio.get_running_loop()
    client = SpinelProtocol()
    conversation = Conversation(loop)

    client_transport = PairedTransport(conversation, loop)
    server_transport = PairedTransport(client, loop)

    client.connection_made(client_transport)
    conversation.connection_made(server_transport)

    return client, conversation


async def test_ignore_duplicate_response(caplog: pytest.LogCaptureFixture) -> None:
    """Test that duplicate responses are ignored."""
    client, conversation = await create_spinel_test_pair()

    # Send a command and get the future
    fut = asyncio.create_task(client.send_command(CommandID.PROP_VALUE_GET, b"\x01"))
    # Let the command be sent
    await asyncio.sleep(0.01)

    assert len(client._pending_frames) == 1
    tid = list(client._pending_frames.keys())[0]

    # Receive the first response
    response1 = SpinelFrame(
        header=SpinelHeader(transaction_id=tid, network_link_id=0, flag=0b10),
        command_id=CommandID.PROP_VALUE_IS,
        data=b"\x01test",
    )
    await conversation.send(HDLCLiteFrame(data=response1.serialize()).serialize())

    with caplog.at_level(logging.DEBUG):
        # Receive the duplicate response immediately
        response2 = SpinelFrame(
            header=SpinelHeader(transaction_id=tid, network_link_id=0, flag=0b10),
            command_id=CommandID.PROP_VALUE_IS,
            data=b"\x01test2",
        )

        await conversation.send(HDLCLiteFrame(data=response2.serialize()).serialize())
        await asyncio.sleep(0.01)

    # The future should be resolved with the first response
    async with asyncio.timeout(1):
        result = await fut

    assert result == response1
    assert f"Ignoring duplicate response for TID {tid}" in caplog.text


async def test_ignore_tid_zero_response(caplog: pytest.LogCaptureFixture) -> None:
    """Test that responses with TID 0 are ignored if no future is pending."""
    client, conversation = await create_spinel_test_pair()

    caplog.set_level(logging.DEBUG)
    caplog.clear()

    # We don't have a pending future for TID 0
    assert 0 not in client._pending_frames

    # Receive a response with TID 0
    response = SpinelFrame(
        header=SpinelHeader(transaction_id=0, network_link_id=0, flag=0b10),
        command_id=CommandID.PROP_VALUE_IS,
        data=b"\x01test",
    )
    await conversation.send(HDLCLiteFrame(data=response.serialize()).serialize())
    await asyncio.sleep(0.01)

    # Nothing should have happened
    for record in caplog.records:
        assert record.levelno < logging.WARNING


@pytest.mark.parametrize(
    "prop_id",
    [
        PropertyID.STREAM_DEBUG,
        0x3D5A,  # VENDOR_ZIGGURAT
    ],
)
async def test_property_listener(prop_id: PropertyID | int) -> None:
    """Test the property listener mechanism."""
    client, conversation = await create_spinel_test_pair()
    listener = MagicMock()
    prop_data = b"hello world"

    client.add_property_listener(prop_id, listener)

    # Send a PROP_VALUE_IS frame
    frame_data = PackedUInt21(prop_id).serialize() + prop_data
    response = SpinelFrame(
        header=SpinelHeader(transaction_id=0, network_link_id=0, flag=0b10),
        command_id=CommandID.PROP_VALUE_IS,
        data=frame_data,
    )
    await conversation.send(HDLCLiteFrame(data=response.serialize()).serialize())
    await asyncio.sleep(0.01)

    assert listener.mock_calls == [call(prop_data)]

    # Test removing the listener
    listener.reset_mock()
    client.remove_property_listener(prop_id, listener)
    await conversation.send(HDLCLiteFrame(data=response.serialize()).serialize())
    await asyncio.sleep(0.01)

    assert listener.mock_calls == []


async def test_iter_property_changes() -> None:
    """Test the async iterator for property changes."""
    client, conversation = await create_spinel_test_pair()
    prop_id = PropertyID.STREAM_DEBUG
    queue: asyncio.Queue[bytes] = asyncio.Queue()

    async def _iterator_task():
        async for value in client.iter_property_changes(prop_id):
            await queue.put(value)

    task = asyncio.create_task(_iterator_task())

    # Send a few PROP_VALUE_IS frames
    for i in range(5):
        response = SpinelFrame(
            header=SpinelHeader(transaction_id=0, network_link_id=0, flag=0b10),
            command_id=CommandID.PROP_VALUE_IS,
            data=PackedUInt21(prop_id).serialize() + f"test {i}".encode("ascii"),
        )
        await conversation.send(HDLCLiteFrame(data=response.serialize()).serialize())

    async with asyncio.timeout(1):
        results = [await queue.get() for _ in range(5)]

    assert results == [f"test {i}".encode("ascii") for i in range(5)]

    task.cancel()


async def test_send_frame_tid_zero_raises() -> None:
    """Test that send_frame with tid=0 and wait_response=True raises ValueError."""
    client, _ = await create_spinel_test_pair()
    frame = SpinelFrame(
        header=SpinelHeader(transaction_id=0, network_link_id=0, flag=0b10),
        command_id=CommandID.NOOP,
        data=b"",
    )

    with pytest.raises(ValueError, match="Cannot wait for response on TID=0 frames"):
        await client.send_frame(frame, tid=0, wait_response=True)

    # Should not raise when wait_response is False
    await client.send_frame(frame, tid=0, wait_response=False)


async def test_tid_concurrency() -> None:
    """Test TID concurrency."""
    client, conversation = await create_spinel_test_pair()

    async def echo_requests() -> None:
        while True:
            request = await conversation.receive()
            await conversation.send_frame(
                SpinelFrame(
                    header=request.header,
                    command_id=CommandID.PROP_VALUE_IS,
                    data=request.data,
                )
            )

    payloads = [f"req-{i}".encode() for i in range(25)]
    tasks = [
        asyncio.create_task(client.send_command(CommandID.PROP_VALUE_GET, payload))
        for payload in payloads
    ]
    await asyncio.sleep(0.01)

    # Only 15 can be in flight, the remaining 10 block waiting for a free TID
    assert sorted(client._pending_frames) == list(range(1, 16))
    assert client._free_tids.empty()
    assert not any(task.done() for task in tasks)

    echo = asyncio.create_task(echo_requests())
    try:
        async with asyncio.timeout(1):
            results = await asyncio.gather(*tasks)
    finally:
        echo.cancel()

    assert [result.data for result in results] == payloads
    assert not client._pending_frames
    assert client._free_tids.qsize() == 15


async def reply_to_next_request(conversation: Conversation, data: bytes) -> SpinelFrame:
    request = await conversation.receive()
    await conversation.send_frame(
        SpinelFrame(
            header=request.header,
            command_id=CommandID.PROP_VALUE_IS,
            data=data,
        )
    )

    return request


@pytest.mark.parametrize(
    "prop_id",
    [
        PropertyID.NCP_VERSION,
        0x3D5A,  # VENDOR_ZIGGURAT
    ],
)
async def test_get_property(prop_id: PropertyID | int) -> None:
    client, conversation = await create_spinel_test_pair()
    prop_bytes = PackedUInt21(prop_id).serialize()

    async with asyncio.timeout(1):
        value, request = await asyncio.gather(
            client.get_property(prop_id),
            reply_to_next_request(conversation, prop_bytes + b"value"),
        )

    assert request.command_id == CommandID.PROP_VALUE_GET
    assert request.data == prop_bytes
    assert value == b"value"


@pytest.mark.parametrize(
    ("reply", "expected"),
    [
        (PackedUInt21(PropertyID.PHY_CHAN).serialize() + b"\x0f", b"\x0f"),
        (PropertyID.LAST_STATUS.serialize() + Status.OK.serialize(), None),
    ],
)
async def test_set_property(reply: bytes, expected: bytes | None) -> None:
    client, conversation = await create_spinel_test_pair()
    prop_bytes = PackedUInt21(PropertyID.PHY_CHAN).serialize()

    async with asyncio.timeout(1):
        value, request = await asyncio.gather(
            client.set_property(PropertyID.PHY_CHAN, b"\x0f"),
            reply_to_next_request(conversation, reply),
        )

    assert request.command_id == CommandID.PROP_VALUE_SET
    assert request.data == prop_bytes + b"\x0f"
    assert value == expected


async def test_set_property_status_error() -> None:
    client, conversation = await create_spinel_test_pair()
    reply = asyncio.create_task(
        reply_to_next_request(
            conversation,
            PropertyID.LAST_STATUS.serialize() + Status.INVALID_ARGUMENT.serialize(),
        )
    )

    async with asyncio.timeout(1):
        with pytest.raises(PropertyStatusError) as exc_info:
            await client.set_property(PropertyID.PHY_CHAN, b"\x0f")

        await reply

    assert exc_info.value.status == Status.INVALID_ARGUMENT


@pytest.mark.parametrize(
    "status",
    [
        Status.PROP_NOT_FOUND,
        0x3C01,  # Vendor
        0x4000 + 1,  # Stack native
    ],
)
async def test_get_property_status_error(status: Status | int) -> None:
    client, conversation = await create_spinel_test_pair()
    reply = asyncio.create_task(
        reply_to_next_request(
            conversation,
            PropertyID.LAST_STATUS.serialize() + PackedUInt21(status).serialize(),
        )
    )

    async with asyncio.timeout(1):
        with pytest.raises(PropertyStatusError) as exc_info:
            await client.get_property(0x3D5A)

        await reply

    assert exc_info.value.property_id == 0x3D5A
    assert exc_info.value.status == status
    assert isinstance(exc_info.value.status, Status) == isinstance(status, Status)


async def test_get_property_unexpected_property() -> None:
    client, conversation = await create_spinel_test_pair()
    reply = asyncio.create_task(
        reply_to_next_request(conversation, PropertyID.HWADDR.serialize() + b"value")
    )

    async with asyncio.timeout(1):
        with pytest.raises(ValueError, match="Unexpected response for property"):
            await client.get_property(PropertyID.NCP_VERSION)

        await reply


@pytest.mark.parametrize(
    ("value", "serialized"),
    [
        (0, b"\x00"),
        (42, b"\x2a"),
        (200, b"\xc8\x01"),
        (1337, b"\xb9\x0a"),
        (40000, b"\xc0\xb8\x02"),
        (2097151, b"\xff\xff\x7f"),
    ],
)
def test_packed_uint21_serialization(value, serialized):
    """Test PackedUInt21 serialization."""

    assert PackedUInt21(value).serialize() == serialized

    deserialized, remainder = PackedUInt21.deserialize(serialized + b"remainder")
    assert deserialized == value
    assert remainder == b"remainder"


@pytest.mark.parametrize("value", [-1, PackedUInt21.max_value + 1])
def test_packed_uint21_serialization_failure(value):
    """Test PackedUInt21 serialization failure on out-of-range values."""

    with pytest.raises(ValueError):
        PackedUInt21(value).serialize()


@pytest.mark.parametrize(
    ("status", "serialized"),
    [
        (Status.OK, b"\x00"),
        (Status.RESET_POWER_ON, b"\x70"),
    ],
)
def test_status_serialization(status, serialized):
    assert status.serialize() == serialized
    assert Status.deserialize(serialized + b"remainder") == (status, b"remainder")


def test_packed_uint21_deserialization_failure():
    """Test PackedUInt21 deserialization failure on oversized input."""

    with pytest.raises(ValueError):
        PackedUInt21.deserialize(b"\x80\x80\x80\x80\x00")
