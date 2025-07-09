import httpx
import urllib.parse


class TrackerClient:
    __slots__ = (
        "announce_url",
        "info_hash",
        "peer_id",
        "port",
        "total_length",
        "uploaded",
        "downloaded",
        "left",
        "interval",
        "min_interval",
        "tracker_id",
        "complete",
        "incomplete",
    )

    def __init__(
        self,
        announce_url: str,
        info_hash: bytes,
        peer_id: bytes,
        port: int,
        total_length: int,
    ):
        self.announce_url = announce_url
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.port = port
        self.total_length = total_length
        self.uploaded = 0
        self.downloaded = 0
        self.left = total_length
        self.interval = None
        self.min_interval = None
        self.tracker_id = None
        self.complete = None
        self.incomplete = None

    def _percent_encode(self, bytes: bytes) -> str:
        safe_chars = (
            "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-_~"
        )
        encoded = urllib.parse.quote_from_bytes(bytes, safe=safe_chars)
        return encoded
