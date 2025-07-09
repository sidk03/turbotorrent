import hashlib
import bencodepy
from pathlib import Path
from src.torrent.metadata import TorrentFile, TorrentMetadata


def parse_torrent_file(path: Path):
    with path.open("rb") as f:
        metainfo = bencodepy.decode(f.read())

    announce = metainfo[b"announce"].decode("utf-8")
    info = metainfo[b"info"]
    info_hash = hashlib.sha1(bencodepy.encode(info)).digest()
    piece_length = info[b"piece length"]
    pieces_raw = info[b"pieces"]
    pieces = [pieces_raw[i : i + 20] for i in range(0, len(pieces_raw), 20)]
    name = info[b"name"].decode("utf-8")

    files = []
    offset = 0
    if b"files" in info:
        for file_dict in info[b"files"]:
            length = file_dict[b"length"]
            path_segments = [seg.decode("utf-8") for seg in file_dict[b"path"]]
            files.append(TorrentFile(path_segments, length, offset))
            offset += length
        total_length = offset
    else:
        length = info[b"length"]
        files.append(TorrentFile([name], length, 0))
        total_length = length

    return TorrentMetadata(
        announce=announce,
        piece_length=piece_length,
        pieces=pieces,
        info_hash=info_hash,
        files=files,
        total_length=total_length,
        name=name,
    )
