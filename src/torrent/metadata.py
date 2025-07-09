class TorrentFile:
    __slots__ = ("path", "length", "offset")

    def __init__(self, path: list[str], length: int, offset: int):
        self.path = path
        self.length = length
        self.offset = offset


class TorrentMetadata:
    __slots__ = (
        "announce",
        "piece_length",
        "pieces",
        "info_hash",
        "files",
        "total_length",
        "name",
    )

    def __init__(
        self,
        announce: str,
        piece_length: int,
        pieces: list[bytes],
        info_hash: bytes,
        files: list[TorrentFile],
        total_length: int,
        name: str,
    ):
        self.announce = announce
        self.piece_length = piece_length
        self.pieces = pieces
        self.info_hash = info_hash
        self.files = files
        self.total_length = total_length
        self.name = name
