from src.eventloop.client import TorrentClient
import asyncio
import bitarray


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
        self.bitfield = bitarray(client.pieces)

        # Processing
        self.send_queue = asyncio.Queue()
        self.response_queue = asyncio.Queue()
        self.block_futures: dict[tuple[int, int], asyncio.Future] = {}
        self.request_tasks: dict[tuple[int, int], asyncio.Task]

        # Client
        self.client = client
