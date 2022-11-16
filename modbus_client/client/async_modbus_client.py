from typing import List, Sequence, Union

from modbus_client.client.async_modbus_base_client import AsyncModbusBaseClient
from modbus_client.client.registers import IRegister
from modbus_client.client.requests import ReadRegistersRequest, SimpleRequest
from modbus_client.client.types import ModbusReadSession


class AsyncModbusClient(AsyncModbusBaseClient):
    async def execute_simple_request(
        self, request: SimpleRequest
    ) -> Union[None | List[bool] | List[int]]:
        return await request.execute(self)

    async def execute_read_registers_request(
        self, request: ReadRegistersRequest
    ) -> ModbusReadSession:
        return await request.execute(self)

    async def read_registers(
        self,
        slave: int,
        registers: Sequence[IRegister],
        allow_holes: bool = True,
        max_read_size: int = 100,
    ) -> ModbusReadSession:
        return await self.execute_read_registers_request(
            ReadRegistersRequest(registers, slave, allow_holes, max_read_size)
        )


__all__ = ["AsyncModbusClient"]
