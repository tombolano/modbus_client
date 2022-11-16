from abc import abstractmethod
from typing import Any, List


class AsyncModbusBaseClient:
    @abstractmethod
    async def write_coil(self, slave: int, address: int, value: bool) -> None:
        pass

    @abstractmethod
    async def read_coils(
        self, slave: int, address: int, count: int
    ) -> List[bool]:
        pass

    @abstractmethod
    async def read_discrete_inputs(
        self, slave: int, address: int, count: int
    ) -> List[int]:
        pass

    @abstractmethod
    async def read_input_registers(
        self, slave: int, address: int, count: int
    ) -> List[int]:
        pass

    @abstractmethod
    async def read_holding_registers(
        self, slave: int, address: int, count: int
    ) -> List[int]:
        pass

    @abstractmethod
    async def write_holding_registers(
        self, slave: int, address: int, values: List[int]
    ) -> None:
        pass

    @abstractmethod
    async def connect(self) -> None:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass

    async def __aenter__(self) -> 'AsyncModbusBaseClient':
        await self.connect()
        return self

    async def __aexit__(
        self, exc_type: Any, exc_val: Any, exc_tb: Any
    ) -> bool:
        await self.close()
        return False
