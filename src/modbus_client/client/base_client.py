from abc import abstractmethod
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Dict, Generic, List, Literal, Sequence, TypeVar

from modbus_client.client.address_range import AddressRange, merge_address_ranges
from modbus_client.client.constants import Defaults
from modbus_client.client.registers import IRegister, RegisterType
from modbus_client.client.types import ModbusReadSession

T = TypeVar("T")


class AsyncModbusBaseClient:
    async def execute(self, request: "Request[T]") -> T:
        return await request.execute(self)

    async def read_registers(
        self,
        registers: Sequence[IRegister],
        slave: int = Defaults.slave,
        allow_holes: bool = Defaults.allow_holes,
        max_read_size: int = Defaults.max_read_size,
    ) -> ModbusReadSession:
        return await self.execute(
            ReadRegistersRequest(registers, slave, allow_holes, max_read_size)
        )

    @abstractmethod
    async def write_coil(
        self, address: int, value: bool, slave: int = Defaults.slave
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def read_coils(
        self,
        address: int,
        count: int = Defaults.count,
        slave: int = Defaults.slave,
    ) -> List[bool]:
        raise NotImplementedError()

    @abstractmethod
    async def read_discrete_inputs(
        self,
        address: int,
        count: int = Defaults.count,
        slave: int = Defaults.slave,
    ) -> List[int]:
        raise NotImplementedError()

    @abstractmethod
    async def read_input_registers(
        self,
        address: int,
        count: int = Defaults.count,
        slave: int = Defaults.slave,
    ) -> List[int]:
        raise NotImplementedError()

    @abstractmethod
    async def read_holding_registers(
        self,
        address: int,
        count: int = Defaults.count,
        slave: int = Defaults.slave,
    ) -> List[int]:
        raise NotImplementedError()

    @abstractmethod
    async def write_holding_registers(
        self, address: int, values: List[int], slave: int = Defaults.slave
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError()

    async def __aenter__(self) -> "AsyncModbusBaseClient":
        await self.connect()
        return self

    async def __aexit__(
        self, exc_type: Any, exc_val: Any, exc_tb: Any
    ) -> Literal[False]:
        await self.close()
        return False


class Request(Generic[T]):
    @abstractmethod
    async def execute(self, client: AsyncModbusBaseClient) -> T:
        raise NotImplementedError()


@dataclass
class WriteCoilRequest(Request[None]):
    T = type(None)
    address: int
    value: bool
    slave: int = Defaults.slave

    async def execute(self, client: AsyncModbusBaseClient) -> None:
        await client.write_coil(self.address, self.value, self.slave)


@dataclass
class ReadCoilsRequest(Request[List[bool]]):
    T = List[bool]
    address: int
    count: int = Defaults.count
    slave: int = Defaults.slave

    async def execute(self, client: AsyncModbusBaseClient) -> List[bool]:
        return await client.read_coils(self.address, self.count, self.slave)


@dataclass
class ReadDiscreteInputsRequest(Request[List[int]]):
    address: int
    count: int = Defaults.count
    slave: int = Defaults.slave

    async def execute(self, client: AsyncModbusBaseClient) -> List[int]:
        return await client.read_discrete_inputs(self.address, self.count, self.slave)


@dataclass
class ReadInputRegistersRequest(Request[List[int]]):
    address: int
    count: int = Defaults.count
    slave: int = Defaults.slave

    async def execute(self, client: AsyncModbusBaseClient) -> List[int]:
        return await client.read_input_registers(self.address, self.count, self.slave)


@dataclass
class ReadHoldingRegistersRequest(Request[List[int]]):
    address: int
    count: int = Defaults.count
    slave: int = Defaults.slave

    async def execute(self, client: AsyncModbusBaseClient) -> List[int]:
        return await client.read_holding_registers(self.address, self.count, self.slave)


@dataclass
class WriteHoldingRegistersRequest(Request[None]):
    address: int
    values: List[int]
    slave: int = Defaults.slave

    async def execute(self, client: AsyncModbusBaseClient) -> None:
        await client.write_holding_registers(self.address, self.values, self.slave)


@dataclass
class ReadRegistersRequest(Request[ModbusReadSession]):
    registers: Sequence[IRegister]
    slave: int = Defaults.slave
    allow_holes: bool = Defaults.allow_holes
    max_read_size: int = Defaults.allow_holes

    def __post_init__(self) -> None:
        self.buckets: Dict[RegisterType, List[AddressRange]] = {}

        for reg_type in RegisterType:
            discrete_reg = (
                reg_type == RegisterType.Coil or reg_type == RegisterType.DiscreteInputs
            )
            self.buckets[reg_type] = merge_address_ranges(
                [x for x in self.registers if x.reg_type == reg_type],
                allow_holes=False if discrete_reg else self.allow_holes,
                max_read_size=1 if discrete_reg else self.max_read_size,
            )

    async def execute(self, client: AsyncModbusBaseClient) -> ModbusReadSession:
        read_functions: Dict[
            RegisterType,
            Callable[..., Coroutine[Any, Any, List[bool] | List[int]]],
        ] = {
            RegisterType.Coil: client.read_coils,
            RegisterType.DiscreteInputs: client.read_discrete_inputs,
            RegisterType.InputRegister: client.read_input_registers,
            RegisterType.HoldingRegister: client.read_holding_registers,
        }

        ses = ModbusReadSession()
        for reg_type in RegisterType:
            for rng in self.buckets[reg_type]:
                result = await read_functions[reg_type](
                    slave=self.slave, address=rng.address, count=rng.count
                )
                for i, val in enumerate(result):
                    ses.registers_dict[(reg_type, rng.address + i)] = val

        return ses


__all__ = [
    "AsyncModbusBaseClient",
    "Request",
    "WriteCoilRequest",
    "ReadCoilsRequest",
    "ReadDiscreteInputsRequest",
    "ReadInputRegistersRequest",
    "ReadHoldingRegistersRequest",
    "WriteHoldingRegistersRequest",
    "ReadRegistersRequest",
]
