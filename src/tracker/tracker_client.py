import httpx
from urllib.parse import urlencode, urlparse
import socket
import struct
import bencodepy
import random
import asyncio
from typing import Optional


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

    def _build_query(self, event: Optional[str] = None) -> str:
        params = {
            "info_hash": self.info_hash,  # bytes; urlencode percent-encodes
            "peer_id": self.peer_id,  # bytes; urlencode percent-encodes
            "port": self.port,
            "uploaded": self.uploaded,
            "downloaded": self.downloaded,
            "left": self.left,
            "compact": 1,
        }
        if event:
            params["event"] = event
        return urlencode(params)

    async def _announce_http(
        self, event: Optional[str] = None
    ) -> list[tuple[str, int]]:
        url = f"{self.announce_url}?{self._build_query(event)}"
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            decoded = bencodepy.decode(response.content)

        if b"failure reason" in decoded:
            raise Exception(f"Tracker failure: {decoded[b'failure reason'].decode()}")

        self.interval = decoded.get(b"interval")
        self.min_interval = decoded.get(b"min interval")
        self.tracker_id = decoded.get(b"tracker id", self.tracker_id)
        self.complete = decoded.get(b"complete")
        self.incomplete = decoded.get(b"incomplete")

        peers_raw = decoded[b"peers"]
        peers = []

        if isinstance(peers_raw, bytes):
            for i in range(0, len(peers_raw), 6):
                ip = socket.inet_ntoa(peers_raw[i : i + 4])
                peer_port = struct.unpack(">H", peers_raw[i + 4 : i + 6])[0]
                peers.append((ip, peer_port))
        else:
            for peer in peers_raw:
                ip = peer[b"ip"].decode()
                peer_port = peer[b"port"]
                peers.append((ip, peer_port))
        return peers

    async def _announce_udp(self, event: Optional[str] = None) -> list[tuple[str, int]]:
        parsed = urlparse(self.announce_url)
        host = parsed.hostname
        port = parsed.port or 80

        ACTION_CONNECT = 0
        ACTION_ANNOUNCE = 1
        MAGIC_CONST = 0x41727101980
        EVENT_MAP = {None: 0, "completed": 1, "started": 2, "stopped": 3}

        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(False)

        max_retries = 8
        base_timeout = 15  # seconds

        # CONNECT with retry
        connection_id = None
        try:
            for retry in range(max_retries):
                timeout = base_timeout * (2**retry)
                transaction_id = random.randint(0, 2**32 - 1)
                connect_msg = struct.pack(
                    "!QLL", MAGIC_CONST, ACTION_CONNECT, transaction_id
                )
                await loop.sock_sendto(sock, connect_msg, (host, port))
                try:
                    resp, _ = await asyncio.wait_for(
                        loop.sock_recvfrom(sock, 4096), timeout
                    )
                except asyncio.TimeoutError:
                    if retry == max_retries - 1:
                        raise Exception(
                            "Failed to get connection ID from UDP tracker (no response)"
                        )
                    continue
                if len(resp) >= 16:
                    action, resp_tid, connection_id_ = struct.unpack("!LLQ", resp[:16])
                    if action == ACTION_CONNECT and resp_tid == transaction_id:
                        connection_id = connection_id_
                        break  # success
            else:
                raise Exception(
                    "No valid response from UDP tracker during CONNECT phase"
                )

            # ANNOUNCE with retry
            for retry in range(max_retries):
                timeout = base_timeout * (2**retry)
                transaction_id = random.randint(0, 2**32 - 1)
                event_id = EVENT_MAP[event]
                num_want = 0xFFFFFFFF  # -1 (as many as possible)
                announce_msg = struct.pack(
                    "!QLL20s20sQQQLLLLH",
                    connection_id,
                    ACTION_ANNOUNCE,
                    transaction_id,
                    self.info_hash,
                    self.peer_id,
                    self.downloaded,
                    self.left,
                    self.uploaded,
                    event_id,
                    0,  # IP address (0 = default)
                    random.randint(0, 2**32 - 1),  # key
                    num_want,
                    self.port,
                )
                await loop.sock_sendto(sock, announce_msg, (host, port))
                try:
                    resp, _ = await asyncio.wait_for(
                        loop.sock_recvfrom(sock, 4096), timeout
                    )
                except asyncio.TimeoutError:
                    if retry == max_retries - 1:
                        raise Exception(
                            "Failed to get announce response from UDP tracker (no response)"
                        )
                    continue
                if len(resp) >= 20:
                    action, resp_tid, interval, leechers, seeders = struct.unpack(
                        "!LLLLL", resp[:20]
                    )
                    if action == ACTION_ANNOUNCE and resp_tid == transaction_id:
                        self.interval = interval
                        self.complete = seeders
                        self.incomplete = leechers

                        peers = []
                        for i in range(20, len(resp), 6):
                            ip = socket.inet_ntoa(resp[i : i + 4])
                            peer_port = struct.unpack(">H", resp[i + 4 : i + 6])[0]
                            peers.append((ip, peer_port))
                        return peers
            else:
                raise Exception(
                    "No valid response from UDP tracker during ANNOUNCE phase"
                )
        finally:
            sock.close()

    async def announce(self, event: str = None) -> list[tuple[str, int]]:
        scheme = self.announce_url.split(":", 1)[0]
        if scheme in {"http", "https"}:
            return await self._announce_http(event)
        elif scheme == "udp":
            return await self._announce_udp(event)
        else:
            raise ValueError(f"Unsupported tracker protocol: {scheme}")

    async def started(self) -> list[tuple[str, int]]:
        return await self.announce(event="started")

    async def completed(self) -> list[tuple[str, int]]:
        self.left = 0
        self.downloaded = self.total_length
        return await self.announce(event="completed")

    async def stopped(self) -> list[tuple[str, int]]:
        return await self.announce(event="stopped")

    async def update(self, downloaded: int, uploaded: int) -> list[tuple[str, int]]:
        self.downloaded = downloaded
        self.uploaded = uploaded
        self.left = max(0, self.total_length - downloaded)
        return await self.announce()
