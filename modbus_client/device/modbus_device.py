from typing import Union

from modbus_client.client.constants import Defaults
from modbus_client.client.base_client import AsyncModbusBaseClient
from modbus_client.client.registers import NumericRegister, Coil
from modbus_client.client.types import RegisterType
from modbus_client.device.device_config import (
    DeviceConfig,
    IDeviceRegister,
    DeviceInputRegister,
    DeviceHoldingRegister,
    load_device_config,
    load_device_config_from_yaml,
    DeviceSwitch,
    SwitchRegisterTypeEnum,
)
from modbus_client.device.device_config_finder import find_device_file


def create_modbus_register(
    device: DeviceConfig, register: IDeviceRegister
) -> NumericRegister:
    zero_offset = 0 if device.zero_mode else 1
    address = register.address - zero_offset

    if isinstance(register, DeviceInputRegister):
        reg_type = RegisterType.InputRegister
    elif isinstance(register, DeviceHoldingRegister):
        reg_type = RegisterType.HoldingRegister
    else:
        raise Exception("invalid type")

    return NumericRegister(
        name=register.name,
        reg_type=reg_type,
        value_type=register.type,
        address=address,
        scale=register.scale,
        unit=register.unit,
    )


def create_modbus_coil(device: DeviceConfig, register: DeviceSwitch) -> Coil:
    zero_offset = 0 if device.zero_mode else 1
    number = register.number - zero_offset

    if register.type == SwitchRegisterTypeEnum.Coil:
        reg_type = RegisterType.Coil
    else:
        raise Exception("invalid type")

    return Coil(name=register.name, reg_type=reg_type, number=number)


class ModbusDevice:
    def __init__(self, device_config: DeviceConfig):
        self._device_config = device_config

    def get_config(self) -> DeviceConfig:
        return self._device_config

    def get_register(self, name: str) -> IDeviceRegister:
        reg = self._device_config.find_register(name)
        assert reg is not None
        return reg

    def get_switch(self, name: str) -> DeviceSwitch:
        switch = self._device_config.find_switch(name)
        assert switch is not None
        return switch

    def get_numeric_register(self, name: str) -> NumericRegister:
        reg = self.get_register(name)
        return create_modbus_register(self._device_config, reg)

    def get_switch_register(self, name: str) -> Coil:
        switch = self.get_switch(name)
        return create_modbus_coil(self._device_config, switch)

    async def read_register(
        self,
        client: AsyncModbusBaseClient,
        register: Union[str, IDeviceRegister],
        slave: int = Defaults.slave,
    ) -> Union[float, int, bool]:
        if isinstance(register, IDeviceRegister):
            modbus_register = create_modbus_register(
                self._device_config, register
            )
        elif isinstance(register, str):
            modbus_register = self.get_numeric_register(register)
        else:
            raise Exception("Invalid register type")

        read_session = await client.read_registers([modbus_register], slave)
        return modbus_register.get_value_from_read_session(read_session)

    async def write_register(
        self,
        client: AsyncModbusBaseClient,
        register: Union[str, IDeviceRegister],
        value: Union[float, int],
        slave: int = Defaults.slave,
    ) -> None:
        if isinstance(register, IDeviceRegister):
            modbus_register = create_modbus_register(
                self._device_config, register
            )
        elif isinstance(register, str):
            modbus_register = self.get_numeric_register(register)
        else:
            raise Exception("Invalid register type")

        modbus_values = modbus_register.value_to_modbus_registers(value)

        await client.write_holding_registers(
            modbus_register.address, modbus_values, slave
        )

    async def read_switch(
        self,
        client: AsyncModbusBaseClient,
        switch: Union[str, DeviceSwitch],
        slave: int = Defaults.slave,
    ) -> bool:
        if isinstance(switch, DeviceSwitch):
            modbus_register = create_modbus_coil(self._device_config, switch)
        elif isinstance(switch, str):
            modbus_register = self.get_switch_register(switch)
        else:
            raise Exception("Invalid switch type")

        read_session = await client.read_registers(
            registers=[modbus_register], slave=slave
        )

        return modbus_register.get_from_read_session(read_session)

    async def switch_set(
        self,
        client: AsyncModbusBaseClient,
        switch: Union[str, DeviceSwitch],
        value: bool,
        slave: int = Defaults.slave,
    ) -> None:
        if isinstance(switch, DeviceSwitch):
            modbus_register = create_modbus_coil(self._device_config, switch)
        elif isinstance(switch, str):
            modbus_register = self.get_switch_register(switch)
        else:
            raise Exception("Invalid register type")

        await client.write_coil(modbus_register.number, value, slave)

    async def switch_toggle(
        self,
        client: AsyncModbusBaseClient,
        switch: Union[str, DeviceSwitch],
        slave: int = Defaults.slave,
    ) -> None:
        current_value = await self.read_switch(client, switch, slave)
        await self.switch_set(client, switch, not current_value, slave)

    @staticmethod
    def create_from_file(path: str) -> "ModbusDevice":
        device = load_device_config(find_device_file(path))
        return ModbusDevice(device)

    @staticmethod
    def create_from_config(config: str) -> "ModbusDevice":
        device = load_device_config_from_yaml(config)
        return ModbusDevice(device)
