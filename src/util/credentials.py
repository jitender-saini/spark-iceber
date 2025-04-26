from dataclasses import dataclass

from util.logging import get_logger

log = get_logger(__name__)


@dataclass
class Credentials:
    username: str
    password: str
