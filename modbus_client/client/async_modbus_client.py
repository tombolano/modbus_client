from abc import abstractmethod
from typing import Any, Callable, Dict, List, Sequence

from modbus_client.client.address_range import merge_address_ranges
from modbus_client.client.registers import IRegister, RegisterType
from modbus_client.client.types import ModbusReadSession


class AsyncModbusClient:
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

    async def __aenter__(self) -> "AsyncModbusClient":
        await self.connect()
        return self

    async def __aexit__(
        self, exc_type: Any, exc_val: Any, exc_tb: Any
    ) -> bool:
        await self.close()
        return False

    async def read_registers(
        self,
        slave: int,
        registers: Sequence[IRegister],
        allow_holes: bool = True,
        max_read_size: int = 100,
    ) -> ModbusReadSession:

        read_functions: Dict[RegisterType, Callable[..., Any]] = {
            RegisterType.Coil: self.read_coils,
            RegisterType.DiscreteInputs: self.read_discrete_inputs,
            RegisterType.InputRegister: self.read_input_registers,
            RegisterType.HoldingRegister: self.read_holding_registers,
        }

        ses = ModbusReadSession()
        for reg_type in RegisterType:
            discrete_reg = (
                reg_type == RegisterType.Coil
                or reg_type == RegisterType.DiscreteInputs
            )
            r = [x for x in registers if x.reg_type == reg_type]
            buckets = merge_address_ranges(
                r,
                allow_holes=False if discrete_reg else allow_holes,
                max_read_size=1 if discrete_reg else max_read_size,
            )
            for rng in buckets:
                res = await read_functions[reg_type](
                    slave=slave, address=rng.address, count=rng.count
                )
                for i, val1 in enumerate(res):
                    ses.registers_dict[(reg_type, rng.address + i)] = val1

        return ses


__all__ = [
    "AsyncModbusClient"
]
