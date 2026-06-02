# aiospinel

An asyncio implementation of the [Spinel](https://datatracker.ietf.org/doc/html/draft-rquattle-spinel-unified)
protocol used to communicate with OpenThread RCPs over a serial connection.

## Installation

```console
pip install aiospinel
```

## Usage

This library exports an `SpinelProtocol` asyncio protocol object.

```python
import asyncio

import serialx

from aiospinel import CommandID, PropertyID, ResetReason, SpinelProtocol


async def main() -> None:
    loop = asyncio.get_running_loop()
    _, protocol = await serialx.create_serial_connection(
        loop=loop,
        protocol_factory=SpinelProtocol,
        url="/dev/ttyUSB0",
        baudrate=460800,
    )
    await protocol.wait_until_connected()

    try:
        await protocol.reset(ResetReason.STACK)

        rsp = await protocol.send_command(
            CommandID.PROP_VALUE_GET,
            PropertyID.HWADDR.serialize(),
        )
        prop_id, hwaddr = PropertyID.deserialize(rsp.data)
        assert prop_id == PropertyID.HWADDR

        print("Hardware address:", hwaddr.hex())
    finally:
        await protocol.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
```
