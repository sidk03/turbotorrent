from src.common.logging import config_logging
from src.torrent.metadata import TorrentMetadata
from src.tracker.tracker_client import TrackerClient
from src.peer.connected_peer import Peer
from pathlib import Path
import bitarray
import random
import os
import asyncio
import time
import logging
import mmap

config_logging("torrent.log.jsonl")

logger = logging.getLogger(__name__)


class Block:
    __slots__ = (
        "piece_index",
        "offset",
        "length",
        "priority",
        "retry_count",
        "last_peer",
    )

    def __init__(self, piece_index: int, offset: int, length: int, priority: int):
        self.piece_index = piece_index
        self.offset = offset
        self.length = length
        self.priority = priority  # Lower = higher priority
        self.retry_count = 0
        self.last_peer = None  # Track which peer last attempted

    def __lt__(self, other):
        return self.priority < other.priority


BLOCK_SIZE = 16384  # 16KB standard block size
ENDGAME_THRESHOLD = 0.98  # Enter endgame at 98% complete
RARITY_CACHE_TTL = 30  # Cache rarity for 30 seconds
MAX_PEERS = 50  # Maximum concurrent peer connections


class TorrentClient:
    __slots__ = (
        "metadata",
        "tracker",
        "info_hash",
        "bitfield",
        "save_path",
        "central_queue",
        "receive_queue",
        "connected_peers",
        "peer_id",
        "file_handles",
        "file_mmaps",
        "piece_buffers",
        "listen_port",
        "global_pending_blocks",
        "rarity_cache",
        "rarity_cache_time",
        "workers",
        "stats",
        "start_time",
        "downloaded",
        "uploaded",
        "endgame_mode",
        "endgame_threshold",
        "block_size",
        "last_piece_length",
        "_shutdown",
    )

    def __init__(self, metadata: TorrentMetadata, save_path: Path):
        self.metadata = metadata
        self.info_hash = metadata.info_hash
        self.save_path = save_path
        self.bitfield = bitarray(len(metadata.pieces))

        # Generate peer ID
        self.peer_id = b"-TT0001-" + os.urandom(12)
        self.listen_port = random.randint(6881, 6889)

        # Queues
        self.central_queue = asyncio.PriorityQueue(maxsize=2000)
        self.receive_queue = asyncio.Queue(maxsize=1000)
        self.connected_peers: list[Peer] = []

        # File management
        self.file_handles = []
        self.file_mmaps: list[tuple[int, int, mmap.mmap]] = []

        # Block management
        self.piece_buffers = {}
        self.global_pending_blocks = {}
        self.block_size = BLOCK_SIZE

        # Calculate last piece length
        last_piece_idx = len(metadata.pieces) - 1
        last_piece_start = last_piece_idx * metadata.piece_length
        self.last_piece_length = metadata.total_length - last_piece_start

        # Piece selection
        self.rarity_cache: list[tuple[int, int]] = []
        self.rarity_cache_time = 0

        # State
        self.downloaded = 0
        self.uploaded = 0
        self.endgame_mode = False
        self.endgame_threshold = ENDGAME_THRESHOLD

        # Workers
        self.workers = []
        self._shutdown = False

        # Stats
        self.start_time = time.time()
        self.stats = {
            "pieces_completed": 0,
            "blocks_requested": 0,
            "blocks_failed": 0,
            "bytes_wasted": 0,
        }

        logger.info(f"Initialized TorrentClient for {metadata.name}")

    async def start(self):
        try:
            self._initialize_storage()

            self.tracker = TrackerClient(
                self.metadata.announce,
                self.info_hash,
                self.peer_id,
                self.listen_port,
                self.metadata.total_length,
            )

            # Get initial peers
            initial_peers = await self.tracker.started()
            logger.info(f"Got {len(initial_peers)} initial peers from tracker")

            # Start workers
            self.workers = [
                asyncio.create_task(self._peer_connector(initial_peers)),
                asyncio.create_task(self._block_scheduler()),
                asyncio.create_task(self._piece_assembler()),
                asyncio.create_task(self._stats_reporter()),
                asyncio.create_task(self._endgame_monitor()),
                asyncio.create_task(self._stale_block_monitor()),
            ]

            await self._run_until_complete()

        except Exception as e:
            logger.error(f"Fatal error in torrent client: {e}")
            raise
        finally:
            await self.cleanup()

    # init storage using mmap for quick writes to vir addr spc
    def _initialize_storage(self):
        if len(self.metadata.files) == 1:
            # Single file torrent
            file_path = self.save_path / self.metadata.files[0].path[0]
            file_path.parent.mkdir(parents=True, exist_ok=True)

            handle = open(file_path, "wb+")
            handle.truncate(self.metadata.total_length)
            handle.flush()

            mmap_obj = mmap.mmap(handle.fileno(), self.metadata.total_length)

            self.file_handles.append(handle)
            self.file_mmaps.append((0, self.metadata.total_length, mmap_obj))

        else:
            # Multi-file torrent
            current_offset = 0
            for file_info in self.metadata.files:
                file_path = self.save_path / Path(*file_info.path)
                file_path.parent.mkdir(parents=True, exist_ok=True)

                handle = open(file_path, "wb+")
                handle.truncate(file_info.length)
                handle.flush()

                mmap_obj = mmap.mmap(handle.fileno(), file_info.length)

                self.file_handles.append(handle)
                self.file_mmaps.append((current_offset, file_info.length, mmap_obj))
                current_offset += file_info.length

        logger.info(f"Initialized storage: {len(self.file_handles)} file(s)")

    def _write_piece(self, offset: int, data: bytes):
        for file_offset, file_length, mmap_obj in self.file_mmaps:
            if file_offset <= offset < file_offset + file_length:
                relative_offset = offset - file_offset
                write_length = min(len(data), file_length - relative_offset)
                mmap_obj[relative_offset : relative_offset + write_length] = data[
                    :write_length
                ]

                if write_length < len(data):
                    # Block spans multiple files
                    self._write_piece(offset + write_length, data[write_length:])
                return

    class TriggerUpdate(Exception):
        def __init__(self, message="Tracker Updated"):
            super().__init__(message)
            logging.info(message)

    async def _peer_connector(self, initial_peers: list[tuple[str, int]]):
        peer_queue = asyncio.Queue()
        # Add initial peers
        for peer_info in initial_peers:
            await peer_queue.put(peer_info)

        connection_semaphore = asyncio.Semaphore(10)  # TCP congestion cntrl
        last_tracker_request = time.time()

        # seen_peers = set()

        async def connect_to_peer(host: str, port: int):
            async with connection_semaphore:
                if len(self.connected_peers) >= MAX_PEERS:
                    return

            # if (host,port) in seen_peers:
            #     return

            if any(p.host == host and p.port == port for p in self.connected_peers):
                return

            try:
                peer = Peer(host, port, self)
                await peer.send_connection()
                self.connected_peers.append(peer)
                # seen_peers.add((host,port))
                logger.info(
                    f"Connected to peer {host}:{port}. Total peers: {len(self.connected_peers)}"
                )
            except Exception as e:
                logger.debug(f"Failed to connect to {host}:{port}: {e}")

        while not self._shutdown:
            try:
                peer_info = await asyncio.wait_for(peer_queue.get(), timeout=1.0)
                asyncio.create_task(connect_to_peer(*peer_info))
                if time.time() - last_tracker_request > (self.tracker.interval or 300):
                    raise TorrentClient.TriggerUpdate(
                        f"Update message sent to tracker at time {time.time()}"
                    )  # triggers interval update

            except (asyncio.TimeoutError, TorrentClient.TriggerUpdate):
                # asks for more peers after connecting to all inital peers
                if (peer_queue.empty() and len(self.connected_peers) < 20) or (
                    time.time() - last_tracker_request > (self.tracker.interval or 300)
                ):
                    try:
                        new_peers = await self.tracker.update(
                            self.downloaded, self.uploaded
                        )
                        last_tracker_request = time.time()
                        for peer_info in new_peers:
                            await peer_queue.put(peer_info)
                        logger.info(f"Added {len(new_peers)} peers to queue")
                    except Exception as e:
                        logger.error(f"Failed to get peers from tracker: {e}")

    async def _block_scheduler(self):
        scheduled_pieces = set()
        while not self._shutdown and not self._is_complete():
            try:
                if self.endgame_mode:  # schedule all blocks remaining
                    for piece_idx in range(len(self.metadata.pieces)):
                        if not self.bitfield[piece_idx]:
                            await self._schedule_piece_blocks(piece_idx, priority=-1000)
                    await asyncio.sleep(5)
                    continue

                # not endgame -> get rarest blocks
                needed_pieces = self.get_needed_pieces_by_rarity()

                for priority, piece_idx in needed_pieces[:25]:
                    if piece_idx not in scheduled_pieces:
                        await self._schedule_piece_blocks(piece_idx, priority)

                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"Block scheduler error: {e}")
                await asyncio.sleep(1)

    async def _schedule_piece_blocks(self, piece_idx: int, priority: int):
        piece_length = self._get_piece_length(piece_idx)
        for offset in range(0, piece_length, self.block_size):
            block_length = min(self.block_size, piece_length - offset)
            block = Block(
                piece_index=piece_idx,
                offset=offset,
                length=block_length,
                priority=priority,
            )

            # this condition dosent hit only in endgame mode
            if (piece_idx, offset) not in self.global_pending_blocks:
                await self.central_queue.put((priority, block))
                self.stats["blocks_requested"] += 1

    def _get_piece_length(self, piece_idx: int) -> int:
        return (
            self.metadata.piece_length
            if piece_idx != len(self.metadata.pieces) - 1
            else self.last_piece_length
        )

    def _get_needed_pieces_by_rarity(self) -> list[int]:
        curr_time = time.time()

        # use cache if valid
        if curr_time - self.rarity_cache_time < RARITY_CACHE_TTL:
            return [
                (avail, idx)
                for avail, idx in self.rarity_cache
                if not self.bitfield[idx]
            ]
        # recalculate
        piece_availability = [0] * len(self.metadata.pieces)
        for peer in self.connected_peers:
            if peer.bitfield:
                for i, has_piece in enumerate(peer.bitfield):
                    if has_piece:
                        self.piece_availability[i] += 1

        self.rarity_cache = []
        for i, availability in enumerate(piece_availability):
            if not self.bitfield[i] and availability > 0:
                self.rarity_cache.append((availability, i))

        self.rarity_cache.sort(key=lambda x: (x[0], random.random()))
        self.rarity_cache_time = curr_time

        return self.rarity_cache

    async def _piece_assembler(self):
        while not self._shutdown:
            try:
                block, data = await self.receive_queue.get()

                if self.bitfield[block.piece_index]:
                    logger.debug(
                        f"Received block for already completed piece {block.piece_index}, discarding"
                    )
                    self.stats["bytes_wasted"] += len(data)
                    continue

                self.downloaded += len(data)

                # rm for global in flight
                block_id = (block.piece_index, block.offset)
                self.global_pending_blocks.pop(block_id, None)

                # Storing
                piece_index = block.piece_index
                if piece_index not in self.piece_buffers:
                    self.piece_buffers[piece_index] = {}

                self.piece_buffers[piece_index][block.offset] = data

                if self._is_piece_complete(piece_index):
                    piece_data = self._assemble_piece(piece_index)

                    if self._verify_piece(piece_index, piece_data):
                        # write to disk
                        piece_offset = piece_index * self.metadata.piece_length
                        self._write_piece(piece_offset, piece_data)

                        self.bitfield[piece_index] = 1
                        self.stats["pieces_completed"] += 1

                        # notify peers
                        # await self._broadcast_have(piece_index)

                        del self.piece_buffers[piece_index]

                        logger.info(
                            f"Piece {piece_index} completed. Progress: {self.stats['pieces_completed']}/{len(self.metadata.pieces)} "
                            f"({100 * self.stats['pieces_completed'] / len(self.metadata.pieces):.1f}%)"
                        )
                    else:
                        logger.warning(f"Piece {piece_index} failed hash verification")

                        # clear and reschedule
                        del self.piece_buffers[piece_index]
                        await self._schedule_piece_blocks(piece_index, priority=-500)
            except Exception as e:
                logger.error(f"Piece assembler error: {e}")

    def _is_complete(self) -> bool:
        return self.bitfield.all()

    async def cleanup(self):
        self._shutdown = True

        for worker in self.workers:
            worker.cancel()

        for peer in self.connected_peers:
            await peer.cleanup()

        for mmap_obj in self.file_mmaps:
            mmap_obj[2].close()

        for handle in self.file_handles:
            handle.close()

        try:
            await self.tracker.stopped()
        except Exception:
            pass

        logger.info("Cleanup complete")
