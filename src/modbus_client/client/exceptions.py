from pymodbus.pdu import ModbusResponse


class ReadErrorException(Exception):
    def __init__(self, response: ModbusResponse) -> None:
        super().__init__(str(response))
        self.response = response


class WriteErrorException(Exception):
    def __init__(self, response: ModbusResponse) -> None:
        super().__init__(str(response))
        self.response = response


__all__ = [
    "ReadErrorException",
    "WriteErrorException",
]
