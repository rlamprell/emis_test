from dataclasses    import dataclass
from abc            import ABC


@dataclass
class db_connection_config(ABC):
    config = {
        "abstract-class": "blank"
    }