from src.eventloop.client import TorrentClient
import asyncio
from bitarray import bitarray
import struct
import math
import logging
import time

logger = logging.getLogger(__name__)


class Peer:
    __slots__ = (
        # Network State
        "host",
        "port",
        "peer_id",
        "reader",
        "writer",
        "connected",
        # Bit protocol State
        "am_choking",
        "am_interested",
        "peer_choking",
        "peer_intertsted",
        "bitfield",
        # Processing
        "pending_requests",
        "max_concurrent",
        "score",
        # Stats
        "last_message_time",
        # Client
        "client",
        # Workers
        "workers",
    )

    def __init__(self, host: str, port: int, client: TorrentClient):
        # Network State
        self.host = host
        self.port = port
        self.peer_id: str = None
        self.reader: asyncio.StreamReader = None
        self.writer: asyncio.StreamWriter = None
        self.connected = False

        # Bit protocol State
        self.am_choking = True
        self.am_interested = False
        self.peer_choking = True
        self.peer_interested = False
        self.bitfield: bitarray = None

        # Processing
        self.pending_requests : dict[tuple[int,int], asyncio.Future] = {}
        self.max_concurrent = 10
        self.score = 1.0

        # Stats
        self.last_message_time = time.time()

        # Client
        self.client = client

        # Workers
        self._workers: list[asyncio.Task] = []

    async def send_connection(self):
        try:
            async with asyncio.timeout(10):
                self.reader, self.writer = await asyncio.open_connection(
                    self.host, self.port
                )

            self._handshake_sequence(initiator=True)

            # start workers
            self._start_workers()

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
            self._handshake_sequence(initiator=False)

            peername = writer.get_extra_info("peername")
            if peername:
                self.host, self.port = peername

            # start workers
            self._start_workers()

        except Exception as e:
            await self._cleanup()
            raise

    
    async def _handshake_sequence(self, initiator: bool):
        if initiator:
            await self._send_handshake()
            await self._receive_handshake()
        else:
            await self._receive_handshake()
            await self._send_handshake()

        self.connected = True
        await self._send_bitfield()

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

    async def _receive_handshake(self):
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
            if self._is_duplicate_peer(peer_id):
                raise ValueError(f"Already connected to peer {peer_id}")

        except asyncio.IncompleteReadError as e:
            raise ValueError(f"Incomplete handshake: received {len(e.partial)} bytes")
        except struct.error as e:
            raise ValueError(f"Invalid handshake format: {e}")

    def _is_duplicate_peer(self, peer_id: bytes) -> bool:
        return any(
            p.peer_id == peer_id and p != self and p.connected
            for p in self.client.connected_peers
        )

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

    def _start_workers(self):
        self._workers = [
            asyncio.create_task(self._request_worker()),
            asyncio.create_task(self._receive_worker()),
            asyncio.create_task(self._keep_alive_worker()),
        ]

    async def _keep_alive_worker(self):
        while self.connected:
            await asyncio.sleep(60)

            if time.time() - self.last_message_time > 120:
                logger.warning(f"Peer {self.host}:{self.port} timed out")
                await self._cleanup()
                break

            try:
                self.writer.write(struct.pack("!I", 0))
                await self.writer.drain()
            except Exception:
                break

    async def _request_worker(self):
        while self.connected:
            try:
                if len(self.pending_requests) >= self.max_concurrent:
                    asyncio.sleep(0.05)
                    continue

                if self.score < 1.0:
                    await asyncio.sleep((1 - self.score) * 0.5)

                block = await self.client.central_queue.get()

                if not self.bitfield or not self.bitfield[block.piece_index]:
                    await self.client.central_queue.put(block)
                    await asyncio.sleep(0.01)  # Prevent tight loop
                    continue

                if self.peer_choking:
                    await self.client.central_queue.put(block)
                    await asyncio.sleep(0.1)  # Wait a bit before trying again
                    continue

                block_id = (block.piece_index, block.offset)

                # Skip if this peer alr has this block in flight
                if block_id in self.pending_requests:
                    await self.client.central_queue.put(block)
                    continue

                # Fire away do not block
                asyncio.create_task(self._request_block(block))

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Request worker error: {e}")
                await asyncio.sleep(1)

    async def _request_block(self, block):
        block_id = (block.piece_index, block.offset)
        start_time = time.time()

        try:
            # Claim block locally
            future = asyncio.Future()
            self.pending_requests[block_id] = future

            message = struct.pack(
                "!IBIII", 13, 6, 
                block.piece_index, block.offset, block.length
            )
            self.writer.write(message)
            await self.writer.drain()

            try:
                data = await asyncio.wait_for(future, timeout=30.0)
                
                # Success
                elapsed = time.time() - start_time
                self._update_stats(success=True, response_time=elapsed)
                
                # Queue for verification
                await self.client.receive_queue.put((block, data))
                
            except asyncio.TimeoutError:
                # Timeout - re-queue with priority
                self._update_stats(success=False)
                
                block.retry_count = getattr(block, 'retry_count', 0) + 1
                block.priority = -1000 - block.retry_count
                await self.client.central_queue.put(block)
                
        except Exception as e:
            logger.error(f"Block request error {block_id}: {e}")
            await self.client.central_queue.put(block)

        finally:
            del self.pending_requests[block_id]



   


    
    async def send_interested(self):
        if not self.am_interested:
            self.am_interested = True
            message = struct.pack("!IB", 1, 2)
            self.writer.write(message)
            await self.writer.drain()

    async def send_not_interested(self):
        if self.am_interested:
            self.am_interested = False
            message = struct.pack("!IB", 1, 3)
            self.writer.write(message)
            await self.writer.drain()

    async def _cleanup(self) -> None:
        self.connected = False

        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

        for task in self.request_tasks.values():
            task.cancel()

        # clear send queue -> add blocks back to main queue
