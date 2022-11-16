from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Sequence, Union

from modbus_client.client.address_range import (
    AddressRange,
    merge_address_ranges,
)
from modbus_client.client.async_modbus_base_client import AsyncModbusBaseClient
from modbus_client.client.registers import IRegister, RegisterType
from modbus_client.client.types import ModbusReadSession


class SimpleRequest:
    @abstractmethod
    async def execute(
        self, client: AsyncModbusBaseClient
    ) -> Union[None | List[bool] | List[int]]:
        pass


@dataclass
class WriteCoilRequest(SimpleRequest):
    slave: int
    address: int
    value: bool

    async def execute(self, client: AsyncModbusBaseClient) -> None:
        await client.write_coil(self.slave, self.address, self.value)


@dataclass
class ReadCoilsRequest(SimpleRequest):
    slave: int
    address: int
    count: int

    async def execute(self, client: AsyncModbusBaseClient) -> None:
        await client.read_coils(self.slave, self.address, self.count)


@dataclass
class ReadDiscreteInputsRequest(SimpleRequest):
    slave: int
    address: int
    count: int

    async def execute(self, client: AsyncModbusBaseClient) -> List[int]:
        return await client.read_discrete_inputs(
            self.slave, self.address, self.count
        )


@dataclass
class ReadInputRegistersRequest(SimpleRequest):
    slave: int
    address: int
    count: int

    async def execute(self, client: AsyncModbusBaseClient) -> List[int]:
        return await client.read_input_registers(
            self.slave, self.address, self.count
        )


@dataclass
class ReadHoldingRegistersRequest(SimpleRequest):
    slave: int
    address: int
    count: int

    async def execute(self, client: AsyncModbusBaseClient) -> List[int]:
        return await client.read_holding_registers(
            self.slave, self.address, self.count
        )


@dataclass
class WriteHoldingRegistersRequest(SimpleRequest):
    slave: int
    address: int
    values: List[int]

    async def execute(self, client: AsyncModbusBaseClient) -> None:
        await client.write_holding_registers(
            self.slave, self.address, self.values
        )


@dataclass
class ReadRegistersRequest:
    registers: Sequence[IRegister]
    slave: int = 1
    allow_holes: bool = True
    max_read_size: int = 100

    def __post_init__(self) -> None:
        self.buckets: Dict[RegisterType, List[AddressRange]] = {}

        for reg_type in RegisterType:
            discrete_reg = (
                reg_type == RegisterType.Coil
                or reg_type == RegisterType.DiscreteInputs
            )
            self.buckets[reg_type] = merge_address_ranges(
                [x for x in self.registers if x.reg_type == reg_type],
                allow_holes=False if discrete_reg else self.allow_holes,
                max_read_size=1 if discrete_reg else self.max_read_size,
            )

    async def execute(
        self, client: AsyncModbusBaseClient
    ) -> ModbusReadSession:
        read_functions: Dict[RegisterType, Callable[..., Any]] = {
            RegisterType.Coil: client.read_coils,
            RegisterType.DiscreteInputs: client.read_discrete_inputs,
            RegisterType.InputRegister: client.read_input_registers,
            RegisterType.HoldingRegister: client.read_holding_registers,
        }

        ses = ModbusReadSession()
        for reg_type in RegisterType:
            for rng in self.buckets[reg_type]:
                res = await read_functions[reg_type](
                    slave=self.slave, address=rng.address, count=rng.count
                )
                for i, val in enumerate(res):
                    ses.registers_dict[(reg_type, rng.address + i)] = val

        return ses
