import asyncio
import concurrent.futures
import functools
from collections.abc import Awaitable, Callable
from typing import Any, Optional, TypeVar, Union, cast

import pymodbus.client
import pymodbus.client.base

from modbus_client.client.base_client import AsyncModbusBaseClient
from modbus_client.client.constants import Defaults
from modbus_client.client.exceptions import ReadErrorException, WriteErrorException

T = TypeVar("T")

pymodbus_client_type = Union[
    pymodbus.client.base.ModbusBaseClient, pymodbus.client.base.ModbusBaseSyncClient
]


class AsyncModbusPyModbusClient(AsyncModbusBaseClient):
    client: pymodbus_client_type
    executor: Optional[concurrent.futures.Executor]

    def __init__(self, client: pymodbus_client_type):
        self.client = client
        self.is_client_async = isinstance(client, pymodbus.client.base.ModbusBaseClient)

        if self.is_client_async:
            self.executor = None
        else:
            self.executor = concurrent.futures.ThreadPoolExecutor(1)

    async def _run(
        self, fn: Callable[..., Awaitable[T] | T], *args: Any, **kwargs: str
    ) -> T:
        if self.is_client_async:
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
    ) -> list[bool]:
        result = await self._run(self.client.read_coils, address, count, slave)
        if result.isError():
            raise ReadErrorException(result)
        return cast(list[bool], result.bits[:count])

    async def read_discrete_inputs(
        self,
        address: int,
        count: int = Defaults.count,
        slave: int = Defaults.slave,
    ) -> list[bool]:
        result = await self._run(
            self.client.read_discrete_inputs, address, count, slave
        )
        if result.isError():
            raise ReadErrorException(result)
        return cast(list[bool], result.bits[:count])

    async def read_input_registers(
        self,
        address: int,
        count: int = Defaults.count,
        slave: int = Defaults.slave,
    ) -> list[int]:
        result = await self._run(
            self.client.read_input_registers, address, count, slave
        )
        if result.isError() or len(result.registers) != count:
            raise ReadErrorException(result)
        return cast(list[int], result.registers)

    async def read_holding_registers(
        self,
        address: int,
        count: int = Defaults.count,
        slave: int = Defaults.slave,
    ) -> list[int]:
        result = await self._run(
            self.client.read_holding_registers, address, count, slave
        )
        if result.isError() or len(result.registers) != count:
            raise ReadErrorException(result)
        return cast(list[int], result.registers)

    async def write_holding_registers(
        self, address: int, values: list[int], slave: int = Defaults.slave
    ) -> None:
        c = self.client
        if len(values) == 1:
            result = await self._run(c.write_register, address, values[0], slave)
        else:
            result = await self._run(c.write_registers, address, values, slave)
        if result.isError():
            raise WriteErrorException(result)

    async def connect(self) -> None:
        await self._run(self.client.connect)

    def close(self) -> None:
        self.client.close()


class ModbusTcpClient(AsyncModbusPyModbusClient):
    def __init__(self, host: str, port: int = Defaults.tcpport, **kwargs: Any) -> None:
        super().__init__(pymodbus.client.ModbusTcpClient(host, port=port, **kwargs))


class AsyncModbusTcpClient(AsyncModbusPyModbusClient):
    def __init__(self, host: str, port: int = Defaults.tcpport, **kwargs: Any) -> None:
        super().__init__(
            pymodbus.client.AsyncModbusTcpClient(host, port=port, **kwargs)
        )


class ModbusSerialClient(AsyncModbusPyModbusClient):
    def __init__(
        self,
        port: str,
        baudrate: int = Defaults.baudrate,
        stopbits: int = Defaults.stopbits,
        parity: str = Defaults.parity,
        **kwargs: Any,
    ):
        super().__init__(
            pymodbus.client.ModbusSerialClient(
                port, baudrate=baudrate, stopbits=stopbits, parity=parity, **kwargs
            )
        )


class AsyncModbusSerialClient(AsyncModbusPyModbusClient):
    def __init__(
        self,
        port: str,
        baudrate: int = Defaults.baudrate,
        stopbits: int = Defaults.stopbits,
        parity: str = Defaults.parity,
        **kwargs: Any,
    ):
        super().__init__(
            pymodbus.client.AsyncModbusSerialClient(
                port, baudrate=baudrate, stopbits=stopbits, parity=parity, **kwargs
            )
        )


__all__ = [
    "ModbusTcpClient",
    "AsyncModbusTcpClient",
    "ModbusSerialClient",
    "AsyncModbusSerialClient",
]
