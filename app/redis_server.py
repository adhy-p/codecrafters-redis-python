import asyncio
import logging
import time

from app.resp_parser import RespParser
from typing import List, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("redis_server")

REPLICATION_ID = b"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"


class RedisServer:
    server: asyncio.Server
    kvstore: dict[bytes, tuple[bytes, int]]
    master: None
    replication_id: bytes
    replication_offset: int

    @classmethod
    async def new(cls, port: int, master: tuple[str, int] | None = None):
        self = cls()
        self.server = await asyncio.start_server(
            self.connection_handler, "localhost", port
        )
        logger.info("initialising server")
        self.kvstore = {}
        self.master = None
        if master:
            master_host, master_port = master
            reader, writer = await asyncio.open_connection(master_host, master_port)
            await RedisServer.init_handshake(reader, writer, port)
            self.master = (master, (reader, writer))
        self.replication_id = REPLICATION_ID
        self.replication_offset = 0
        return self

    @staticmethod
    async def init_handshake(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter, port: int
    ):
        PING_CMD = b"*1\r\n$4\r\nping\r\n"
        logger.info("[worker] initialising handshake with master")
        logger.info("[worker] sending PING")
        writer.write(PING_CMD)
        await writer.drain()
        _resp: bytes = await reader.read(1024)
        logger.info(f"[worker] received {_resp!r}")
        # ignore the server's response for now
        # todo: check _resp == PING response
        logger.info("[worker] sending first REPLCONF")
        writer.write(
            RedisServer._encode_command(
                [b"REPLCONF", b"listening-port", RedisServer._int_to_bytestr(port)]
            )
        )
        await writer.drain()
        _resp: bytes = await reader.read(1024)
        logger.info(f"[worker] received {_resp!r}")
        logger.info("[worker] sending second REPLCONF")
        writer.write(RedisServer._encode_command([b"REPLCONF", b"capa", b"psync2"]))
        await writer.drain()
        _resp: bytes = await reader.read(1024)
        logger.info(f"[worker] received {_resp!r}")

        logger.info("[worker] sending PSYNC")
        writer.write(
            RedisServer._encode_command(
                [b"PSYNC", b"?", RedisServer._int_to_bytestr(-1)]
            )
        )
        await writer.drain()
        _resp: bytes = await reader.read(1024)
        logger.info(f"[worker] received {_resp!r}")

    async def connection_handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        while True:
            data: bytes = await reader.read(1024)
            addr: str = writer.get_extra_info("peername")

            if not data:
                logger.info(f"closing connection with {addr}")
                writer.close()
                await writer.wait_closed()
                return

            requests = RespParser.parse_request(data)
            if requests:
                logger.info(f"received {requests!r} from {addr!r}")
            for req in requests:
                resp = await self.handle_request(req)
                if not resp:
                    continue
                writer.write(resp)
                logger.info(f"replied {resp!r} to client")
                await writer.drain()

    @staticmethod
    def _int_to_bytestr(i: int) -> bytes:
        return str(i).encode("utf-8")

    @staticmethod
    def _encode_command(args: List[bytes]) -> bytes:
        args = [RedisServer._encode_bulkstr(msg) for msg in args]
        arr_len = len(args)
        return b"*" + RedisServer._int_to_bytestr(arr_len) + b"\r\n" + b"".join(args)

    @staticmethod
    def _encode_bulkstr(msg: bytes) -> bytes:
        msg_len = len(msg)
        return b"$" + RedisServer._int_to_bytestr(msg_len) + b"\r\n" + msg + b"\r\n"

    def _handle_psync(self) -> bytes:
        EMPTY_RDB = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        rdb_file = RedisServer._encode_bulkstr(bytes.fromhex(EMPTY_RDB)).rstrip(
            b"\r\n"
        )  # rdb does not contain a \r\n at the end

        return (
            b"+FULLRESYNC "
            + self.replication_id
            + b" "
            + RedisServer._int_to_bytestr(0)
            + b"\r\n"
            + rdb_file
        )

    def _handle_info(self, req: List[bytes]) -> bytes:
        query: bytes = req[1]
        if query.upper() != b"REPLICATION":
            return b"-Currently only supporting replication for INFO command\r\n"
        # maybe change to a dictionary once the number of fields grows
        role = b"role:" + (b"master" if not self.master else b"slave")
        repl_id = b"master_replid:" + self.replication_id
        offset = b"master_repl_offset:" + RedisServer._int_to_bytestr(
            self.replication_offset
        )
        return RedisServer._encode_bulkstr(b"\n".join([role, repl_id, offset]))

    def _handle_get(self, req: List[bytes]) -> bytes:
        key: bytes = req[1]
        value: Tuple[bytes, int] | None = self.kvstore.get(key)
        if value:
            msg, expiry = value
            if expiry == -1 or time.time_ns() // 1_000_000 <= expiry:
                return RedisServer._encode_bulkstr(msg)
        return b"$-1\r\n"

    def _handle_set(self, req: List[bytes]) -> bytes:
        key: bytes = req[1]
        val: bytes = req[2]
        expiry: int = -1
        if len(req) > 3:
            precision: bytes = req[3]
            if precision.upper() != b"PX":
                return b"-Currently only supporting PX for SET timeout\r\n"
            expiry = time.time_ns() // 1_000_000 + int(req[4].decode())
        self.kvstore[key] = (val, expiry)
        return b"+OK\r\n"

    async def handle_request(self, req: List[bytes]) -> bytes:
        assert len(req) > 0
        match req[0].upper():
            case b"PING":
                return b"+PONG\r\n"
            case b"ECHO":
                if len(req) < 2:
                    return b"-Missing argument(s) for ECHO\r\n"
                return RedisServer._encode_bulkstr(req[1])
            case b"SET":
                if len(req) < 3:
                    return b"-Missing argument(s) for SET\r\n"
                return self._handle_set(req)
            case b"GET":
                if len(req) < 2:
                    return b"-Missing argument(s) for GET\r\n"
                return self._handle_get(req)
            case b"INFO":
                if len(req) < 2:
                    return b"-Missing argument(s) for INFO\r\n"
                return self._handle_info(req)
            case b"REPLCONF":
                return b"+OK\r\n"
            case b"PSYNC":
                return self._handle_psync()
            case _:
                logger.error(f"Received {req[0]!r} command (not supported)!")
                return b"-Command not supported yet!\r\n"

    async def serve(self):
        async with self.server:
            await self.server.serve_forever()
