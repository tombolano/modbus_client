from typing import List, cast, Any

import pymodbus.bit_read_message
import pymodbus.client
import pymodbus.register_read_message
import pymodbus.register_write_message

from modbus_client.client.exceptions import (
    ReadErrorException, WriteErrorException)
from modbus_client.client.async_modbus_client import AsyncModbusClient


class PyAsyncModbusClient(AsyncModbusClient):
    def __init__(self, client: pymodbus.client.base.ModbusBaseClient):
        if not client.use_protocol:
            raise ValueError("client must be a pymodbus asynchronous client object")
        self.client = client

    async def write_coil(self, slave: int, address: int, value: bool) -> None:
        await self.client.write_coil(slave=slave, address=address, value=value)

    async def read_coils(self, slave: int, address: int,
                         count: int) -> List[bool]:
        bytes_count = (count + 7) // 8
        result = await self.client.read_coils(slave=slave, address=address, count=bytes_count)
        if not result.isError():
            if result.byte_count != bytes_count:
                raise ReadErrorException

            return cast(List[bool], result.bits[:count])

        raise ReadErrorException

    async def read_discrete_inputs(self, slave: int, address: int, count: int) -> List[int]:
        result = await self.client.read_discrete_inputs(slave=slave, address=address, count=count)
        if not result.isError():
            if result.byte_count != count:
                raise ReadErrorException

            values = []
            for byte_i in range(count):
                value = 0
                start_bit = 8 * byte_i
                end_bit = start_bit + 8
                for i, bit in enumerate(result.bits[start_bit:end_bit]):
                    value |= bit << i
                values.append(value)
            return values
        else:
            raise ReadErrorException(str(result))

    async def read_input_registers(self, slave: int, address: int, count: int) -> List[int]:
        result = await self.client.read_input_registers(slave=slave, address=address, count=count)
        if not result.isError():
            if len(result.registers) != count:
                raise ReadErrorException

            return cast(List[int], result.registers)
        else:
            raise ReadErrorException(str(result))

    async def read_holding_registers(self, slave: int, address: int, count: int) -> List[int]:
        result = await self.client.read_holding_registers(slave=slave, address=address, count=count)
        if not result.isError():
            if len(result.registers) != count:
                raise ReadErrorException

            return cast(List[int], result.registers)
        else:
            raise ReadErrorException(str(result))

    async def write_holding_registers(self, slave: int, address: int,
                                      values: List[int]) -> None:
        if len(values) == 1:
            result = await self.client.write_register(slave=slave, address=address, value=values[0])
        else:
            result = await self.client.write_registers(slave=slave, address=address, values=values)

        if result.isError():
            raise WriteErrorException(str(result))

    async def connect(self) -> None:
        await self.client.connect()

    async def close(self) -> None:
        await self.client.close()


class PyAsyncModbusTcpClient(PyAsyncModbusClient):
    def __init__(self, host: str, port: int = 502, **kwargs: Any) -> None:
        super().__init__(pymodbus.client.tcp.AsyncModbusTcpClient(
            host=host, port=port, **kwargs))


class PyAsyncModbusRtuClient(PyAsyncModbusClient):
    def __init__(self, path: str, baudrate: int = 9600, stopbits: int = 1, parity: str = "N", **kwargs: Any):
        super().__init__(pymodbus.client.serial.AsyncModbusSerialClient(
            port=path, baudrate=baudrate, stopbits=stopbits, parity=parity, **kwargs))


__all__ = [
    "PyAsyncModbusRtuClient",
    "PyAsyncModbusTcpClient"
]
