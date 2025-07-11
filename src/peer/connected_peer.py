from src.eventloop.client import TorrentClient
import asyncio
from bitarray import bitarray
import struct
import math
import logging

logger = logging.getLogger(__name__)


class Peer:
    __slots__ = (
        # Network State
        "host",
        "port",
        "peer_id",
        "reader",
        "writer",
        "running",
        # Bit protocol State
        "am_choking",
        "am_interested",
        "peer_choking",
        "peer_intertsted",
        "bitfield",
        # Processing
        "send_queue",
        "response_queue",
        "block_futures",
        "request_tasks",
        # Client
        "client",
    )

    def __init__(self, host: str, port: int, client: TorrentClient):
        # Network State
        self.host = host
        self.port = port
        self.peer_id: str = None
        self.reader: asyncio.StreamReader = None
        self.writer: asyncio.StreamWriter = None
        self.running = False

        # Bit protocol State
        self.am_choking = True
        self.am_interested = False
        self.peer_choking = True
        self.peer_interested = False
        self.bitfield: bitarray = None
        # Processing
        self.send_queue = asyncio.Queue()
        self.response_queue = asyncio.Queue()
        self.block_futures: dict[tuple[int, int], asyncio.Future] = {}
        self.request_tasks: dict[tuple[int, int], asyncio.Task] = {}

        # Client
        self.client = client

    async def send_connection(self):
        try:
            async with asyncio.timeout(10):
                self.reader, self.writer = await asyncio.open_connection(
                    self.host, self.port
                )
            await self._send_handshake()
            await self._accept_handshake()

            await self._send_bitfield()

            self.running = True

            # start workers

        except asyncio.TimeoutError:
            print(f"Connection timeout to {self.host}:{self.port} -> Outgoing")
            await self._cleanup()
            raise
        except Exception as e:
            await self._cleanup()
            raise

    async def accept_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        self.reader = reader
        self.writer = writer
        try:
            await self._accept_handshake()
            await self._send_handshake()

            await self._send_bitfield()

            self.running = True

            peername = writer.get_extra_info("peername")
            if peername:
                self.host, self.port = peername

            # start workers

        except Exception as e:
            await self._cleanup()
            raise

    async def _send_handshake(self):
        # len, pstr, reserved, info_hash, my id
        handshake = struct.pack(
            "!B19s8s20s20s",
            19,
            "BitTorrent protocol",
            b"\x00" * 8,
            self.client.metadata.info_hash,
            self.client.tracker.peer_id,
        )
        self.writer.write(handshake)
        await self.writer.drain()

    async def _accept_handshake(self):
        try:
            # Read exactly 68 bytes for handshake
            async with asyncio.timeout(5):
                handshake_data = await self.reader.readexactly(68)

            # Unpack handshake
            pstrlen, pstr, _, info_hash, peer_id = struct.unpack(
                "!B19s8s20s20s", handshake_data
            )

            # Validate handshake
            if pstrlen != 19:
                raise ValueError(f"Invalid pstrlen: {pstrlen}")

            if pstr != b"BitTorrent protocol":
                raise ValueError(f"Invalid protocol string: {pstr}")

            if info_hash != self.client.info_hash:
                raise ValueError(f"Info hash mismatch")

            # Store peer ID
            self.peer_id = peer_id

            # Decide what to do with duplicate connections !!!

        except asyncio.IncompleteReadError as e:
            raise ValueError(f"Incomplete handshake: received {len(e.partial)} bytes")
        except struct.error as e:
            raise ValueError(f"Invalid handshake format: {e}")

    async def _send_bitfield(self):
        if not self.client.bitfield.any():
            print(f"Skipping bitfield message: no pieces available.")
            return
        bitfield_bytes = self.client.bitfield.tobytes()
        message_id = 5
        message_length = 1 + len(bitfield_bytes)
        bitfield_message = struct.pack(
            f"!IB{len(bitfield_bytes)}s", message_length, message_id, bitfield_bytes
        )
        self.writer.write(bitfield_message)
        await self.writer.drain()

    async def _recieve_bitfield(self):
        try:
            length_prefix_bytes = await self.reader.readexactly(4)
            (length_prefix,) = struct.unpack("!I", length_prefix_bytes)
            if length_prefix < 1:
                raise ValueError("Bitfield message too short")

            message_id_bytes = await self.reader.readexactly(1)
            (message_id,) = struct.unpack("!B", message_id_bytes)
            if message_id != 5:
                raise ValueError(f"Expected bitfield (5), got {message_id}")

            # len check
            bitfield_length = length_prefix - 1
            num_pieces = self.client.metadata.pieces
            expected_length = math.ceil(num_pieces / 8)

            if bitfield_length != expected_length:
                raise ValueError(
                    f"Bitfield wrong length: expected {expected_length}, got {bitfield_length}"
                )

            bitfield_bytes = await self.reader.readexactly(bitfield_length)

            # check if any spare bits are set, if so error
            num_spare_bits = (8 - (num_pieces % 8)) % 8
            if num_spare_bits:
                last_byte = bitfield_bytes[-1]
                spare_mask = (1 << num_spare_bits) - 1
                if last_byte & spare_mask:
                    raise ValueError(
                        f"Bitfield has spare bits set: last_byte=0b{last_byte:08b}, mask=0b{spare_mask:08b}"
                    )

            bitfield = bitarray(endian="big")
            bitfield.frombytes(bitfield_bytes)
            bitfield = bitfield[:num_pieces]

            self.bitfield = bitfield

            print(
                f"Received valid bitfield from {self.host}:{self.port}: {self.bitfield}"
            )

        except (asyncio.IncompleteReadError, ValueError) as e:
            print(f"Bitfield error from {self.host}:{self.port}: {e}")
            await self._cleanup()
            raise
        except Exception as e:
            print(f"Unexpected error receiving bitfield: {e}")
            await self._cleanup()
            raise

    async def _cleanup(self) -> None:
        self.running = False

        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

        for task in self.request_tasks.values():
            task.cancel()

        # clear send queue -> add blocks back to main queue
