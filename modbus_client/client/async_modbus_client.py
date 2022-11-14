from abc import abstractmethod
from typing import List, Sequence

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

    async def read_registers(
            self, unit: int, registers: Sequence[IRegister],
            allow_holes: bool = True) -> ModbusReadSession:

        coils_registers = \
            [x for x in registers if
             x.reg_type == RegisterType.Coil]
        discrete_inputs_registers = \
            [x for x in registers if
             x.reg_type == RegisterType.DiscreteInputs]
        input_registers = \
            [x for x in registers if
             x.reg_type == RegisterType.InputRegister]
        holding_registers = \
            [x for x in registers if
             x.reg_type == RegisterType.HoldingRegister]

        coils_buckets = merge_address_ranges(
            coils_registers, allow_holes=False, max_read_size=1)
        discrete_inputs_buckets = merge_address_ranges(
            discrete_inputs_registers, allow_holes=False, max_read_size=1)
        input_registers_buckets = merge_address_ranges(
            input_registers, allow_holes=allow_holes, max_read_size=100)
        holding_registers_buckets = merge_address_ranges(
            holding_registers, allow_holes=allow_holes, max_read_size=100)

        ses = ModbusReadSession()
        for rng in coils_buckets:
            coil_data = await self.read_coils(
                unit=unit, address=rng.address, count=rng.count)
            for i, val1 in enumerate(coil_data):
                ses.registers_dict[
                    (RegisterType.Coil, rng.address + i)] = val1

        for rng in discrete_inputs_buckets:
            discrete_inputs_data = await self.read_discrete_inputs(
                unit=unit, address=rng.address, count=rng.count)
            for i, val2 in enumerate(discrete_inputs_data):
                ses.registers_dict[
                    (RegisterType.DiscreteInputs, rng.address + i)] = val2

        for rng in input_registers_buckets:
            input_registers_data = await self.read_input_registers(
                unit=unit, address=rng.address, count=rng.count)
            for i, val3 in enumerate(input_registers_data):
                ses.registers_dict[
                    (RegisterType.InputRegister, rng.address + i)] = val3

        for rng in holding_registers_buckets:
            holding_registers_data = await self.read_holding_registers(
                unit=unit, address=rng.address, count=rng.count)
            for i, val4 in enumerate(holding_registers_data):
                ses.registers_dict[
                    (RegisterType.HoldingRegister, rng.address + i)] = val4

        return ses


__all__ = [
    "AsyncModbusClient"
]
