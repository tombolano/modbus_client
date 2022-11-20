from dataclasses import dataclass
from typing import ClassVar


@dataclass
class Defaults:
    allow_holes: ClassVar[bool] = True
    baudrate: ClassVar[int] = 19200
    count: ClassVar[int] = 1
    max_read_size: ClassVar[int] = 100
    parity: ClassVar[str] = "N"
    slave: ClassVar[int] = 1
    stopbits: ClassVar[int] = 1
    tcpport: ClassVar[int] = 502
