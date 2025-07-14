from src.common.logging import config_logging

config_logging("torrent.log.jsonl")


class Block:
    __slots__ = (
        "piece_index",
        "offset",
        "length",
        "priority",
        "retry_count",
        "last_peer",
    )

    def __init__(self, piece_index: int, offset: int, length: int):
        self.piece_index = piece_index
        self.offset = offset
        self.length = length
        self.priority = 0  # Lower = higher priority
        self.retry_count = 0
        self.last_peer = None  # Track which peer last attempted


class TorrentClient:
    pass
