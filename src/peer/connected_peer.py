from src.eventloop.client import TorrentClient, Block
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
        "peer_interested",
        "bitfield",
        # Processing
        "pending_requests",
        "max_concurrent",
        "score",
        "semaphore",
        # Adaptive throttling
        "skip_next_throttle",
        "consecutive_successes",
        "consecutive_failures",
        # Performance
        "stats",
        "last_message_time",
        # Client
        "client",
        # Workers
        "_workers",
    )

    def __init__(self, host: str, port: int, client: TorrentClient):
        # Network State
        self.host = host
        self.port = port
        self.peer_id: bytes = None
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
        self.pending_requests: dict[tuple[int, int], tuple[asyncio.Future, Block]] = {}
        self.max_concurrent = 30  # Fixed maximum concurrency
        self.score = 1.0
        self.semaphore = asyncio.Semaphore(30)  # Fixed semaphore capacity
        
        # Adaptive throttling
        self.skip_next_throttle = False  # Skip throttle if last block was invalid
        self.consecutive_successes = 0
        self.consecutive_failures = 0

        # Stats
        self.stats = {
            "completed": 0,
            "timeouts": 0,
            "bytes_downloaded": 0,
        }
        self.last_message_time = time.time()

        # Client
        self.client = client

        # Workers
        self._workers: list[asyncio.Task] = []

    async def send_connection(self):
        logger.info(f"Initiating outbound connection to peer {self.host}:{self.port}")
        try:
            async with asyncio.timeout(10):
                self.reader, self.writer = await asyncio.open_connection(
                    self.host, self.port
                )
            logger.info(f"TCP connection established to {self.host}:{self.port}")

            await self._handshake_sequence(initiator=True)
            logger.info(
                f"Handshake completed successfully with peer {self.host}:{self.port} (peer_id: {self.peer_id.hex()[:8]}...)"
            )

            # start workers
            self._start_workers()
            logger.info(f"Started worker tasks for peer {self.host}:{self.port}")

        except Exception as e:
            logger.error(
                f"Connection failed for outbound peer {self.host}:{self.port}: {e}"
            )
            await self.cleanup()
            raise

    async def accept_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        peername = writer.get_extra_info("peername")
        if peername:
            self.host, self.port = peername
        logger.info(f"Accepting inbound connection from peer {self.host}:{self.port}")

        self.reader = reader
        self.writer = writer
        try:
            await self._handshake_sequence(initiator=False)
            logger.info(
                f"Handshake completed successfully with inbound peer {self.host}:{self.port} (peer_id: {self.peer_id.hex()[:8]}...)"
            )

            # start workers
            self._start_workers()
            logger.info(
                f"Started worker tasks for inbound peer {self.host}:{self.port}"
            )

        except Exception as e:
            logger.error(
                f"Connection failed for inbound peer {self.host}:{self.port}: {e}"
            )
            await self.cleanup()
            raise

    async def _handshake_sequence(self, initiator: bool):
        if initiator:
            await self._send_handshake()
            await self._receive_handshake()
        else:
            await self._receive_handshake()
            await self._send_handshake()

        self.connected = True
        logger.info(
            f"Peer {self.host}:{self.port} connection established, sending bitfield"
        )
        await self._send_bitfield()

    async def _send_handshake(self):
        logger.info(f"Sending handshake to peer {self.host}:{self.port}")
        # len, pstr, reserved, info_hash, my id
        handshake = struct.pack(
            "!B19s8s20s20s",
            19,
            b"BitTorrent protocol",
            b"\x00" * 8,
            self.client.metadata.info_hash,
            self.client.tracker.peer_id,
        )
        self.writer.write(handshake)
        await self.writer.drain()
        logger.info(f"Handshake sent to {self.host}:{self.port}")

    async def _receive_handshake(self):
        logger.info(f"Waiting for handshake from peer {self.host}:{self.port}")
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
            logger.info(
                f"Received valid handshake from {self.host}:{self.port} (peer_id: {peer_id.hex()[:8]}...)"
            )

            # Decide what to do with duplicate connections !!!
            if self._is_duplicate_peer():
                logger.warning(
                    f"Duplicate connection detected for peer {peer_id.hex()[:8]}... at {self.host}:{self.port}"
                )
                raise ValueError(f"Already connected to peer {peer_id}")

        except asyncio.IncompleteReadError as e:
            raise ValueError(f"Incomplete handshake: received {len(e.partial)} bytes")
        except struct.error as e:
            raise ValueError(f"Invalid handshake format: {e}")

    def _is_duplicate_peer(self) -> bool:
        return any(
            ((p.host == self.host and p.port==self.port) or p.peer_id == self.peer_id) and p != self and p.connected
            for p in self.client.connected_peers
        )

    async def _send_bitfield(self):
        if not self.client.bitfield.any():
            logger.info(
                f"Skipping bitfield transmission to {self.host}:{self.port}: no pieces available to advertise"
            )
            return
        bitfield_bytes = self.client.bitfield.tobytes()
        message_id = 5
        message_length = 1 + len(bitfield_bytes)
        bitfield_message = struct.pack(
            f"!IB{len(bitfield_bytes)}s", message_length, message_id, bitfield_bytes
        )
        self.writer.write(bitfield_message)
        await self.writer.drain()

    async def _receive_bitfield(self, payload: bytes):
        try:
            num_pieces = len(self.client.metadata.pieces)
            expected_length = math.ceil(num_pieces / 8)

            if len(payload) != expected_length:
                raise ValueError(
                    f"Bitfield wrong length: expected {expected_length}, got {len(payload)}"
                )

            # check if any spare bits are set, if so error
            num_spare_bits = (8 - (num_pieces % 8)) % 8
            if num_spare_bits:
                last_byte = payload[-1]
                spare_mask = (1 << num_spare_bits) - 1
                if last_byte & spare_mask:
                    raise ValueError(
                        f"Bitfield has spare bits set: last_byte=0b{last_byte:08b}, mask=0b{spare_mask:08b}"
                    )

            bitfield = bitarray(endian="big")
            bitfield.frombytes(payload)
            bitfield = bitfield[:num_pieces]

            self.bitfield = bitfield

            logger.info(
                f"Successfully received bitfield from {self.host}:{self.port} with {self.bitfield.count()} pieces available out of {len(self.bitfield)} total"
            )

        except (asyncio.IncompleteReadError, ValueError) as e:
            logger.warning(
                f"Bitfield validation error from {self.host}:{self.port}: {e}"
            )
            await self.cleanup()
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error receiving bitfield from {self.host}:{self.port}: {e}"
            )
            await self.cleanup()
            raise

    def _start_workers(self):
        self._workers = [
            asyncio.create_task(self._request_worker()),
            asyncio.create_task(self._receive_worker()),
            asyncio.create_task(self._keep_alive_worker()),
        ]

    async def _keep_alive_worker(self):
        logger.info(f"Keep-alive worker started for peer {self.host}:{self.port}")
        while self.connected:
            await asyncio.sleep(60)

            if time.time() - self.last_message_time > 120:
                logger.warning(
                    f"Peer {self.host}:{self.port} timed out (no messages for {time.time() - self.last_message_time:.1f}s)"
                )
                await self.cleanup()
                break

            try:
                logger.info(f"Sending keep-alive to peer {self.host}:{self.port}")
                self.writer.write(struct.pack("!I", 0))
                await self.writer.drain()
            except Exception as e:
                logger.error(f"Keep-alive failed for peer {self.host}:{self.port}: {e}")
                break
        logger.info(f"Keep-alive worker stopped for peer {self.host}:{self.port}")

    async def _request_worker(self):
        logger.info(f"Request worker started for peer {self.host}:{self.port}")
        acq = False
        while self.connected:
            try:
                # Apply throttling BEFORE acquiring semaphore and getting block
                if not self.skip_next_throttle:
                    await self._apply_adaptive_throttle()
                else:
                    # Reset the skip flag
                    self.skip_next_throttle = False
                
                await self.semaphore.acquire()
                acq = True

                priority, block = await self.client.central_queue.get()

                # Check if peer has this piece
                if not self.bitfield or not self.bitfield[block.piece_index]:
                    await self.client.central_queue.put((priority, block))
                    self.semaphore.release()
                    acq = False
                    self.skip_next_throttle = True  # Don't throttle next time - invalid block
                    await asyncio.sleep(0.01)  # Prevent tight loop
                    continue

                # Check if peer is choking us
                if self.peer_choking:
                    logger.debug(
                        f"Peer {self.host}:{self.port} is choking us, re-queuing block {block.piece_index}:{block.offset}"
                    )
                    await self.client.central_queue.put((priority, block))
                    self.semaphore.release()
                    acq = False
                    self.skip_next_throttle = True  # Don't throttle next time - peer choking
                    await asyncio.sleep(0.1)  # Wait a bit before trying again
                    continue

                block_id = (block.piece_index, block.offset)

                # Skip if this peer already has this block in flight
                if block_id in self.pending_requests:
                    self.semaphore.release()
                    acq = False
                    await self.client.central_queue.put((priority, block))
                    self.skip_next_throttle = True  # Don't throttle next time - duplicate
                    continue

                # Mark block as globally pending
                self.client.global_pending_blocks[block_id] = (time.time(), block)

                logger.debug(
                    f"Requesting block from {self.host}:{self.port}: piece {block.piece_index}, offset {block.offset}, length {block.length}"
                )
                # Fire away do not block
                asyncio.create_task(self._request_block(block))
                
                # Valid block requested - don't skip throttle next time
                self.skip_next_throttle = False

            except Exception as e:
                if acq:
                    self.semaphore.release()
                logger.error(
                    f"Request worker error for peer {self.host}:{self.port}: {e}"
                )
                await asyncio.sleep(1)
        logger.info(f"Request worker stopped for peer {self.host}:{self.port}")

    async def _apply_adaptive_throttle(self):
        """Apply adaptive throttling based on peer performance score."""
        if self.score < 0.2:
            # Very poor performance - long delay
            delay = 2.0
        elif self.score < 0.4:
            # Poor performance - significant delay
            delay = 1.0
        elif self.score < 0.6:
            # Below average - moderate delay
            delay = 0.5
        elif self.score < 0.8:
            # Average - small delay
            delay = 0.2
        elif self.score < 0.95:
            # Good - minimal delay
            delay = 0.05
        else:
            # Excellent - no delay
            delay = 0
        
        if delay > 0:
            logger.debug(
                f"Throttling peer {self.host}:{self.port} for {delay:.2f}s (score: {self.score:.2f})"
            )
            await asyncio.sleep(delay)

    async def _request_block(self, block):
        block_id = (block.piece_index, block.offset)
        start_time = time.time()

        try:
            # Claim block locally
            future = asyncio.Future()
            self.pending_requests[block_id] = (future, block)

            message = struct.pack(
                "!IBIII", 13, 6, block.piece_index, block.offset, block.length
            )
            self.writer.write(message)
            await self.writer.drain()

            try:
                data = await asyncio.wait_for(future, timeout=30.0)

                # Success
                elapsed = time.time() - start_time
                logger.info(
                    f"Block request successful for {self.host}:{self.port}: piece {block.piece_index}, offset {block.offset}, size {block.length}, elapsed {elapsed:.3f}s"
                )
                self._update_stats(success=True, response_time=elapsed)

                # Queue for verification
                await self.client.receive_queue.put((block, data))

            except asyncio.TimeoutError:
                # Timeout - re-queue with priority
                logger.warning(
                    f"Block request timeout for {self.host}:{self.port}: piece {block.piece_index}, offset {block.offset}, retry_count {getattr(block, 'retry_count', 0)}"
                )
                self._update_stats(success=False)

                block.retry_count = getattr(block, "retry_count", 0) + 1
                block.priority = -1000 - block.retry_count
                await self.client.central_queue.put((block.priority, block))

        except Exception as e:
            logger.error(f"Block request error {block_id}: {e}")
            await self.client.central_queue.put((block.priority, block))

        finally:
            self.semaphore.release()
            self.pending_requests.pop(block_id, None)

    async def _receive_worker(self):
        logger.info(f"Receive worker started for peer {self.host}:{self.port}")
        buffer = bytearray(65536)  # one 64KB buffer for efficiency
        buffer_view = memoryview(buffer)

        while self.connected:
            try:
                # msg length
                await self.reader.readinto(buffer_view[:4])
                (length,) = struct.unpack("!I", buffer_view[:4])

                if length == 0:  # Keep-alive
                    logger.info(
                        f"Received keep-alive from peer {self.host}:{self.port}"
                    )
                    self.last_message_time = time.time()
                    continue

                # msg id
                await self.reader.readinto(buffer_view[:1])
                msg_id = buffer_view[0]

                # payload if payload
                payload_length = length - 1
                if payload_length > 0:
                    if payload_length <= 65531:  # Fits in buffer
                        await self.reader.readinto(buffer_view[:payload_length])
                        payload = buffer_view[:payload_length].tobytes()
                    else:  # Large payload
                        logger.info(
                            f"Receiving large payload from {self.host}:{self.port}: {payload_length} bytes"
                        )
                        payload = await self.reader.readexactly(payload_length)
                else:
                    payload = b""

                self.last_message_time = time.time()
                await self._handle_message(msg_id, payload)

            except asyncio.IncompleteReadError:
                logger.error(
                    f"Peer {self.host}:{self.port} disconnected (incomplete read)"
                )
                break
            except Exception as e:
                logger.error(
                    f"Receive worker error for peer {self.host}:{self.port}: {e}"
                )
                break

        logger.info(f"Receive worker stopped for peer {self.host}:{self.port}")
        await self.cleanup()

    async def _handle_message(self, msg_id: int, payload: bytes):
        if msg_id == 0:  # Choke
            logger.info(f"Peer {self.host}:{self.port} choked us")
            self.peer_choking = True

        elif msg_id == 1:  # Unchoke
            logger.info(f"Peer {self.host}:{self.port} unchoked us")
            self.peer_choking = False
            # Send interested if we need pieces
            if not self.am_interested and self._need_pieces():
                await self.send_interested()

        elif msg_id == 2:  # Interested
            logger.info(
                f"Peer {self.host}:{self.port} expressed interest in our pieces"
            )
            self.peer_interested = True

        elif msg_id == 3:  # Not interested
            logger.info(
                f"Peer {self.host}:{self.port} is no longer interested in our pieces"
            )
            self.peer_interested = False

        elif msg_id == 4:  # Have
            (piece_index,) = struct.unpack("!I", payload)
            if self.bitfield and 0 <= piece_index < len(self.bitfield):
                logger.info(
                    f"Peer {self.host}:{self.port} announced having piece {piece_index}"
                )
                self.bitfield[piece_index] = 1
                # Send interested if we need pieces
                if not self.am_interested and self._need_pieces():
                    await self.send_interested()

        elif msg_id == 5:  # Bitfield
            logger.info(f"Received bitfield message from {self.host}:{self.port}")
            await self._receive_bitfield(payload)
            # Send interested if we need pieces
            if not self.am_interested and self._need_pieces():
                await self.send_interested()

        elif msg_id == 6:  # Request Block, will fill in later
            logger.info(
                f"Received block request from {self.host}:{self.port} (not implemented)"
            )
            pass

        elif msg_id == 7:  # Piece
            logger.info(f"Received piece data message from {self.host}:{self.port}")
            self._handle_piece(payload)

    def _handle_piece(self, payload: bytes):
        if len(payload) < 8:
            logger.warning(
                f"Received invalid piece message from {self.host}:{self.port}: payload too short ({len(payload)} bytes)"
            )
            return

        piece_index, offset = struct.unpack_from("!II", payload, 0)
        data = payload[8:]

        logger.info(
            f"Received piece data from {self.host}:{self.port}: piece {piece_index}, offset {offset}, size {len(data)} bytes"
        )

        block_id = (piece_index, offset)
        future, _ = self.pending_requests.get(block_id, (None, None))
        if future and not future.done():
            future.set_result(data)
            self.stats["bytes_downloaded"] += len(data)
            logger.info(
                f"Successfully matched piece data to pending request for {self.host}:{self.port}: piece {piece_index}, offset {offset}"
            )
        else:
            logger.warning(
                f"Received unrequested piece data from {self.host}:{self.port}: piece {piece_index}, offset {offset}"
            )

    def _need_pieces(self):
        return bool((self.bitfield & ~self.client.bitfield).any())

    def _update_stats(self, success: bool, response_time: float = 0):
        if success:
            self.stats["completed"] += 1
            self.consecutive_successes += 1
            self.consecutive_failures = 0
            
            logger.info(
                f"Block request completed for {self.host}:{self.port}: response_time={response_time:.3f}s, score={self.score:.2f}, completed={self.stats['completed']}"
            )

            # Increase score gradually
            if self.consecutive_successes < 5:
                # Small increase for initial successes
                self.score = min(1.0, self.score + 0.02)
            elif self.consecutive_successes < 20:
                # Moderate increase for consistent performance
                self.score = min(1.0, self.score + 0.03)
            else:
                # Larger increase for excellent performance
                self.score = min(1.0, self.score + 0.05)
            
            # Log significant improvements
            if self.consecutive_successes == 10:
                logger.info(
                    f"Peer {self.host}:{self.port} showing consistent good performance (10 consecutive successes, score: {self.score:.2f})"
                )
            elif self.consecutive_successes == 50:
                logger.info(
                    f"Peer {self.host}:{self.port} showing excellent performance (50 consecutive successes, score: {self.score:.2f})"
                )

        else:
            self.stats["timeouts"] += 1
            self.consecutive_failures += 1
            self.consecutive_successes = 0
            
            logger.warning(
                f"Block request timeout for {self.host}:{self.port}: score={self.score:.2f}, timeouts={self.stats['timeouts']}"
            )

            # Decrease score more aggressively for consecutive failures
            if self.consecutive_failures == 1:
                # First failure - moderate decrease
                self.score = max(0.1, self.score - 0.10)
            elif self.consecutive_failures < 5:
                # Multiple failures - larger decrease
                self.score = max(0.1, self.score - 0.15)
            else:
                # Many failures - severe penalty
                self.score = max(0.1, self.score - 0.25)
            
            # Log significant degradations
            if self.consecutive_failures == 3:
                logger.warning(
                    f"Peer {self.host}:{self.port} experiencing issues (3 consecutive failures, score: {self.score:.2f})"
                )
            elif self.consecutive_failures == 10:
                logger.error(
                    f"Peer {self.host}:{self.port} severely degraded (10 consecutive failures, score: {self.score:.2f})"
                )

    async def send_interested(self):
        if not self.am_interested:
            logger.info(f"Sending interested message to {self.host}:{self.port}")
            self.am_interested = True
            message = struct.pack("!IB", 1, 2)
            self.writer.write(message)
            await self.writer.drain()

    async def send_not_interested(self):
        if self.am_interested:
            logger.info(f"Sending not interested message to {self.host}:{self.port}")
            self.am_interested = False
            message = struct.pack("!IB", 1, 3)
            self.writer.write(message)
            await self.writer.drain()

    async def cleanup(self) -> None:
        self.connected = False

        for task in self._workers:
            task.cancel()

        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass

        if self.pending_requests:
            for _, block in self.pending_requests.values():
                await self.client.central_queue.put((block.priority, block))

        try:
            self.client.connected_peers.remove(self)
        except ValueError:
            pass
