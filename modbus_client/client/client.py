import asyncio
import concurrent
import functools
import pymodbus.client

from collections.abc import Awaitable, Callable
from modbus_client.client.exceptions import (
    ReadErrorException,
    WriteErrorException,
)
from modbus_client.client.constants import Defaults
from modbus_client.client.base_client import AsyncModbusBaseClient
from typing import Any, cast, List, Optional, TypeVar


T = TypeVar("T")


class AsyncModbusPyModbusClient(AsyncModbusBaseClient):
    client: pymodbus.client.ModbusBaseClient
    executor: Optional[concurrent.futures.Executor]

    def __init__(self, client: pymodbus.client.base.ModbusBaseClient):
        if client.use_protocol:
            self.executor = None
        else:
            self.executor = concurrent.futures.ThreadPoolExecutor(1)
        self.client = client

    async def _run(
        self, fn: Callable[..., Awaitable[T] | T], *args: int, **kwargs: str
    ) -> T:
        if self.client.use_protocol:
            fn = cast(Callable[..., Awaitable[T]], fn)
            return await fn(*args, *kwargs)
        else:
            fn = cast(Callable[..., T], fn)
            return await asyncio.get_running_loop().run_in_executor(
                self.executor, functools.partial(fn, *args, **kwargs)
            )

    async def write_coil(
        self, address: int, value: bool, slave: int = Defaults.slave
    ) -> None:
        result = await self._run(self.client.write_coil, address, value, slave)
        if result.isError():
            raise ReadErrorException(result)

    async def read_coils(
        self,
        address: int,
        count: int = Defaults.count,
        slave: int = Defaults.slave,
    ) -> List[bool]:
        bytes_count = (count + 7) // 8
        result = await self._run(
            self.client.read_coils, address, bytes_count, slave
        )
        if result.isError() or result.byte_count != bytes_count:
            raise ReadErrorException(result)
        return cast(List[bool], result.bits[:count])

    async def read_discrete_inputs(
        self,
        address: int,
        count: int = Defaults.count,
        slave: int = Defaults.slave,
    ) -> List[int]:
        result = await self._run(
            self.client.read_discrete_inputs, address, count, slave
        )
        if result.isError() or result.byte_count != count:
            raise ReadErrorException(result)
        values = []
        for byte_i in range(count):
            value = 0
            start_bit = 8 * byte_i
            end_bit = start_bit + 8
            for i, bit in enumerate(result.bits[start_bit:end_bit]):
                value |= bit << i
            values.append(value)
        return values

    async def read_input_registers(
        self,
        address: int,
        count: int = Defaults.count,
        slave: int = Defaults.slave,
    ) -> List[int]:
        result = await self._run(
            self.client.read_input_registers, address, count, slave
        )
        if result.isError() or len(result.registers) != count:
            raise ReadErrorException(result)
        return cast(List[int], result.registers)

    async def read_holding_registers(
        self,
        address: int,
        count: int = Defaults.count,
        slave: int = Defaults.slave,
    ) -> List[int]:
        result = await self._run(
            self.client.read_holding_registers, address, count, slave
        )
        if result.isError() or len(result.registers) != count:
            raise ReadErrorException(result)
        return cast(List[int], result.registers)

    async def write_holding_registers(
        self, address: int, values: List[int], slave: int = Defaults.slave
    ) -> None:
        c = self.client
        f = c.write_register if len(values) == 1 else c.write_registers
        result = await self._run(f, address, values[0], slave)
        if result.isError():
            raise WriteErrorException(result)

    async def connect(self) -> None:
        await self._run(self.client.connect)

    async def close(self) -> None:
        await self._run(self.client.close)


class ModbusTcpClient(AsyncModbusPyModbusClient):
    def __init__(
        self, host: str, port: int = Defaults.tcpport, **kwargs: Any
    ) -> None:
        super().__init__(pymodbus.client.ModbusTcpClient(host, port, **kwargs))


class AsyncModbusTcpClient(AsyncModbusPyModbusClient):
    def __init__(
        self, host: str, port: int = Defaults.tcpport, **kwargs: Any
    ) -> None:
        super().__init__(
            pymodbus.client.AsyncModbusTcpClient(host, port, **kwargs)
        )


class ModbusSerialClient(AsyncModbusPyModbusClient):
    def __init__(
        self,
        port: str,
        baudrate: int = Defaults.baudrate,
        stopbits: int = Defaults.stopbits,
        parity: str = Defaults.parity,
        **kwargs: Any
    ):
        super().__init__(
            pymodbus.client.ModbusSerialClient(
                port,
                baudrate=baudrate,
                stopbits=stopbits,
                parity=parity,
                **kwargs
            )
        )


class AsyncModbusSerialClient(AsyncModbusPyModbusClient):
    def __init__(
        self,
        port: str,
        baudrate: int = Defaults.baudrate,
        stopbits: int = Defaults.stopbits,
        parity: str = Defaults.parity,
        **kwargs: Any
    ):
        super().__init__(
            pymodbus.client.AsyncModbusSerialClient(
                port,
                baudrate=baudrate,
                stopbits=stopbits,
                parity=parity,
                **kwargs
            )
        )


__all__ = [
    "ModbusTcpClient",
    "AsyncModbusTcpClient",
    "ModbusSerialClient",
    "AsyncModbusSerialClient",
]
