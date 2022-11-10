Modbus client for Python
-----

> **Note**
>
> This is a fork of https://github.com/KrystianD/modbus_client to make a Python package which can be installed with `pip`, and also with some other minor changes and fixes. 


Device oriented Modbus client. As opposed to bare modbus clients, it focuses on data meaning and data types. Uses pymodbus under the hood.

#### Features

- Merging read requests
- System config file support (storing devices addresses/paths and their unit numbers in config file for easy querying)

#### Installation

The package can be installed with pip from the repository:

```
pip install git+https://github.com/tombolano/modbus_client.git
```

#### Supported data types:

- S16 (int16) - 16bit signed integer
- U16 (uint16) - 16bit unsigned integer
- S32BE (int32be) - 32bit signed integer, high word first
- S32LE (int32le) - 32bit signed integer, low word first
- U32BE (uint32be) - 32bit unsigned integer, high word first
- U32LE (uint32le) - 32bit unsigned integer, low word first
- F32BE (float32be) - IEEE 754 32bit float, high word first
- F32LE (float32le) - IEEE 754 32bit float, low word first

Within the word, it assumes data is stored Least Significant Bit first.

#### Example

Take an example energy meter device with 2 registers:

- 0x0001 - voltage, 1 word (2 bytes), LSB = 0.1 V
- 0x0002 - energy, 2 words (4 bytes), high word first, LSB = 1 Wh

```
===================
| Address | Value |
===================
|  0x0001 |   123 |
|  0x0002 |     1 |
|  0x0003 |    50 |
===================
```

`config.yaml` file content

```yaml
zero_mode: True # if true, "address: 1" means second register, otherwise, "address: 1" means first register

registers:
  input_registers:
    - name: voltage
      address: 0x0001
      type: uint16
      scale: 0.1
      unit: V

    - name: energy
      address: 0x0002
      type: uint32be
      unit: Wh

    # or in short form:
    # - voltage/0x0001/uint16*0.1[V]
    # - energy/0x0002/uint32be[Wh] 
```

#### Library usage

Reading device YAML file and querying some registers data:
```python
import asyncio
from modbus_client.client.pymodbus_async_modbus_client import PyAsyncModbusTcpClient
from modbus_client.device.modbus_device import ModbusDevice

async def main():
    client = PyAsyncModbusTcpClient(host="192.168.1.10", port=4444, timeout=3)
    # modbus_client = PyAsyncModbusRtuClient(path="/dev/ttyUSB0", baudrate=9600, stopbits=1, parity='N', timeout=3)
    device = ModbusDevice.create_from_file("config.yaml")
    voltage = await device.read_register(client, unit=1, register="voltage")
    energy = await device.read_register(client, unit=1, register="energy")
    print(voltage) # 12.3
    print(energy) # 65586

asyncio.get_event_loop().run_until_complete(main())
```

Directly defining the device registers in Python and querying them:
```python
import asyncio
from modbus_client.client.pymodbus_async_modbus_client import PyAsyncModbusTcpClient
from modbus_client.device.modbus_device import ModbusDevice

async def main():
    R = modbus_client.client.registers.NumericRegister
    T = modbus_client.client.types.RegisterType
    V = modbus_client.client.types.RegisterValueType

    IR = T.InputRegister

    registers = [
        R("voltage", IR, 0x0001, V.U16,   unit="V", scale=0.1),
        R("energy",  IR, 0x0002, V.U32BE, unit="Wh"),
    ]

    client = PyAsyncModbusTcpClient(host="192.168.1.10", port=4444, timeout=3)

    try:
        read_session = await client.read_registers(
            unit=1, registers=registers)

        # Print register data
        for reg in registers:
            val = reg.get_from_read_session(read_session)
            print(f"{reg.name}: {val} {reg.unit}")
    finally:
        client.close()

asyncio.get_event_loop().run_until_complete(main())
```

#### CLI usage:

```bash
python -m modbus_client.cli device config.yaml <connection-params> --unit 1 read voltage
python -m modbus_client.cli device config.yaml <connection-params> --unit 1 read energy
```

