import asyncio
import logging
import time
import abc
import pathlib

from app.resp_parser import RespParser
from app.rdb_parser import RdbParser
from typing import List, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("redis_server")

REPLICATION_ID = b"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"


class RedisServer(abc.ABC):
    server: asyncio.Server
    kvstore: dict[bytes, bytes]
    expirystore: dict[bytes, int]
    rdb_dir: pathlib.Path
    rdb_file: pathlib.Path
    workers: dict[tuple[asyncio.StreamReader, asyncio.StreamWriter], int]
    replication_id: bytes
    replication_offset: int

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

            parsed_requests, _ = RespParser.parse_request(data)
            if parsed_requests:
                logger.info(f"received {parsed_requests!r} from {addr!r}")
            for req in parsed_requests:
                resp = await self.handle_request(req, reader, writer)
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

    def _load_rdb(self) -> tuple[dict[bytes, bytes], dict[bytes, int]]:
        try:
            with open(self.rdb_dir / self.rdb_filename, "rb") as f:
                return RdbParser.parse(f.read())
        except FileNotFoundError:
            logger.info("rdb file not found")
        return {}, {}

    async def _broadcast_to_workers(self, _req: bytes) -> bytes:
        return b""

    async def _handle_rdb_keys(self, req: List[bytes]) -> bytes:
        return RedisServer._encode_command(self.kvstore.keys())

    async def _handle_config(self, req: List[bytes]) -> bytes:
        if req[1].upper() != b"GET":
            logger.info("CONFIG {req[1]!r} not supported yet!")
            return b""
        config_key = req[2]
        # todo: catch byte decoding error
        config_value = self.config.get(config_key.decode("utf-8"))
        config_value = str(config_value).encode("utf-8") if config_value else b""
        return self._encode_command([config_key, config_value])

    async def _wait_acks(self, num_min_acks: int, timeout_ms: int) -> int:
        up_to_date_workers = 0
        end_time_ms = time.time_ns() / 1_000_000 + timeout_ms
        while (
            up_to_date_workers < num_min_acks
            and time.time_ns() / 1_000_000 < end_time_ms
        ):
            up_to_date_workers = self._get_up_to_date_servers()
            # allow other task to run
            await asyncio.sleep(0)
            await asyncio.sleep(0.1)
        return up_to_date_workers

    def _get_up_to_date_servers(self) -> int:
        """
        checks if the master and the replicas has the same replication offset
        """
        up_to_date = 0
        for _, offset in self.workers.items():
            if offset >= self.replication_offset:
                """
                todo: figure out why client's offset is always larger than master's
                """
                up_to_date += 1
        return up_to_date

    async def _handle_wait(self, req: List[bytes]) -> bytes:
        if self.replication_offset == 0:
            return b":" + RedisServer._int_to_bytestr(len(self.workers)) + b"\r\n"

        logger.info("wait: broadcasting getack command")
        broadcasted_msg = await self._broadcast_to_workers(
            [b"REPLCONF", b"GETACK", b"*"]
        )

        logger.info("wait: checking worker's offset")
        min_acks: int = int(req[1])
        timeout_ms: int = int(req[2])
        # we immediately check worker's status
        # if there are enough acks, respond to client immediately
        # else, we wait until there are enough events
        num_acks = await self._wait_acks(min_acks, timeout_ms)
        logger.info(f"updating replication offset to {self.replication_offset}")
        self.replication_offset += len(broadcasted_msg)

        return b":" + RedisServer._int_to_bytestr(num_acks) + b"\r\n"

    def _handle_replconf(
        self,
        req: List[bytes],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> bytes:
        conf_type: bytes = req[1]
        match conf_type.upper():
            case b"GETACK":
                return RedisServer._encode_command(
                    [
                        b"REPLCONF",
                        b"ACK",
                        RedisServer._int_to_bytestr(self.replication_offset),
                    ]
                )
            case b"ACK":
                self.workers[(reader, writer)] = int(req[2])
                return b""
            case b"LISTENING-PORT":
                return b"+OK\r\n"
            case b"CAPA":
                return b"+OK\r\n"
            case _:
                return b""

    def _handle_info(self, role: bytes, req: List[bytes]) -> bytes:
        query: bytes = req[1]
        if query.upper() != b"REPLICATION":
            return b"-Currently only supporting replication for INFO command\r\n"
        # maybe change to a dictionary once the number of fields grows
        repl_id = b"master_replid:" + self.replication_id
        offset = b"master_repl_offset:" + RedisServer._int_to_bytestr(
            self.replication_offset
        )
        return RedisServer._encode_bulkstr(b"\n".join([role, repl_id, offset]))

    def _handle_get(self, req: List[bytes]) -> bytes:
        key: bytes = req[1]
        value: bytes | None = self.kvstore.get(key)
        expiry_ms: int | None = self.expirystore.get(key)
        if value:
            if expiry_ms is None or time.time_ns() // 1_000_000 <= expiry_ms:
                return RedisServer._encode_bulkstr(value)
        return b"$-1\r\n"

    async def _handle_set(self, req: List[bytes]) -> bytes:
        key: bytes = req[1]
        val: bytes = req[2]
        expiry_ms: int | None = None
        if len(req) > 3:
            precision: bytes = req[3]
            if precision.upper() != b"PX":
                return b"-Currently only supporting PX for SET timeout\r\n"
            expiry_ms = time.time_ns() // 1_000_000 + int(req[4].decode())
        self.kvstore[key] = val
        if expiry_ms is not None:
            self.expirystore[key] = expiry_ms
        broadcasted_msg = await self._broadcast_to_workers(req)
        self.replication_offset += len(broadcasted_msg)
        logger.info(f"updating replication offset to {self.replication_offset}")
        return b"+OK\r\n"

    async def handle_request(
        self,
        req: List[bytes],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> bytes:
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
                response = await self._handle_set(req)
                return response
            case b"GET":
                if len(req) < 2:
                    return b"-Missing argument(s) for GET\r\n"
                return self._handle_get(req)
            case b"INFO":
                if len(req) < 2:
                    return b"-Missing argument(s) for INFO\r\n"
                return self._handle_info(req)
            case b"REPLCONF":
                if len(req) < 2:
                    return b"-Missing argument(s) for REPLCONF\r\n"
                return self._handle_replconf(req, reader, writer)
            case b"PSYNC":
                return await self._handle_psync(reader, writer)
            case b"WAIT":
                if len(req) < 3:
                    return b"-Missing argument(s) for WAIT\r\n"
                return await self._handle_wait(req)
            case b"CONFIG":
                if len(req) < 3:
                    return b"-Missing argument(s) for CONFIG\r\n"
                return await self._handle_config(req)
            case b"KEYS":
                return await self._handle_rdb_keys(req)
            case _:
                logger.error(f"Received {req[0]!r} command (not supported)!")
                return b"-Command not supported yet!\r\n"

    async def _parse_and_handle_request(
        self,
        req: bytes,
        _reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> bytes:
        parsed_requests, orig_req_len = RespParser.parse_request(req)
        if parsed_requests:
            logger.info("received requests")
        for req, req_len in zip(parsed_requests, orig_req_len):
            logger.info(f"handling request: {req!r}")
            resp = await self.handle_master_request(req)
            self.replication_offset += req_len
            logger.info(f"updating replication offset to {self.replication_offset}")
            if not resp:
                continue
            writer.write(resp)
            logger.info(f"replied {resp!r} to master")
            await writer.drain()

    @abc.abstractmethod
    async def serve(self):
        pass


class RedisMasterServer(RedisServer):
    @classmethod
    async def new(cls, config: dict[str, Any]):
        self = cls()
        self.server = await asyncio.start_server(
            self.connection_handler, "localhost", config.get("port")
        )
        logger.info("initialising master server...")
        self.kvstore = {}
        self.expirystore = {}
        self.config = config
        self.rdb_dir = pathlib.Path(config.get("dir"))
        self.rdb_filename = pathlib.Path(config.get("dbfilename"))
        kvs, exps = self._load_rdb()
        self.kvstore.update(kvs)
        self.kvstore.update(exps)
        self.workers = {}
        self.replication_id = REPLICATION_ID
        self.replication_offset = 0
        return self

    async def _broadcast_to_workers(self, req: List[bytes]) -> bytes:
        logger.info(f"preparing to broadcast {req!r}")
        command = RedisServer._encode_command(req)
        logger.info(f"broadcasting {command!r} to all replicas")
        dead_workers = []
        for reader, writer in self.workers:
            addr: str = writer.get_extra_info("peername")
            logger.info(f"sending to replica {addr}")
            try:
                writer.write(command)
                await writer.drain()
            except ConnectionResetError:
                logger.info("worker closed the connection. remove from worker list")
                dead_workers.append((reader, writer))
        # clean up dead workers
        for w in dead_workers:
            self.workers.pop(w)
        return command

    async def _handle_psync(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> bytes:
        EMPTY_RDB = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        rdb_file = RedisServer._encode_bulkstr(bytes.fromhex(EMPTY_RDB)).rstrip(
            b"\r\n"
        )  # rdb does not contain a \r\n at the end

        self.workers[(reader, writer)] = 0

        return (
            b"+FULLRESYNC "
            + self.replication_id
            + b" "
            + RedisServer._int_to_bytestr(0)
            + b"\r\n"
            + rdb_file
        )

    def _handle_info(self, req: List[bytes]) -> bytes:
        return super()._handle_info(b"role:master", req)

    async def serve(self):
        async with self.server:
            await self.server.serve_forever()


class RedisWorkerServer(RedisServer):
    master: tuple[asyncio.StreamReader, asyncio.StreamWriter]

    @classmethod
    async def new(cls, config: dict[str, Any]):
        self = cls()
        self.server = await asyncio.start_server(
            self.connection_handler, "localhost", config.get("port")
        )
        logger.info("initialising worker server...")
        self.kvstore = {}
        self.expirystore = {}
        self.config = config
        self.rdb_dir = pathlib.Path(config.get("dir"))
        self.rdb_filename = pathlib.Path(config.get("dbfilename"))
        kvs, exps = self._load_rdb()
        self.kvstore.update(kvs)
        self.kvstore.update(exps)
        self.workers = set()
        self.replication_id = b"?"
        self.replication_offset = -1

        master_host, master_port = config.get("master")
        reader, writer = await asyncio.open_connection(master_host, master_port)
        self.master = (reader, writer)
        await self._init_handshake(reader, writer, config.get("port"))
        return self

    async def _init_handshake(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, port: int
    ):
        PING_CMD = b"*1\r\n$4\r\nping\r\n"
        logger.info("initialising handshake with master")
        logger.info("sending PING")
        writer.write(PING_CMD)
        await writer.drain()
        _resp: bytes = await reader.read(1024)
        logger.info(f"received {_resp!r}")
        # ignore the server's response for now
        # todo: check _resp == PING response

        logger.info("sending first REPLCONF")
        writer.write(
            RedisServer._encode_command(
                [b"REPLCONF", b"listening-port", RedisServer._int_to_bytestr(port)]
            )
        )
        await writer.drain()
        _resp: bytes = await reader.read(1024)
        logger.info(f"received {_resp!r}")

        logger.info("sending second REPLCONF")
        writer.write(RedisServer._encode_command([b"REPLCONF", b"capa", b"psync2"]))
        await writer.drain()
        _resp: bytes = await reader.read(1024)
        logger.info(f"received {_resp!r}")

        logger.info("sending PSYNC")
        writer.write(
            RedisServer._encode_command(
                [
                    b"PSYNC",
                    self.replication_id,
                    RedisServer._int_to_bytestr(self.replication_offset),
                ]
            )
        )
        await writer.drain()
        resp: bytes = await reader.read(1024)
        logger.info(f"received {_resp!r}")

        fullresync_resp, _length, remain = RespParser.parse_simplestr(resp)
        logger.info(f"full resync simplestr: {fullresync_resp!r}")
        cmd_type, id, offset = fullresync_resp.split(b" ")
        assert cmd_type == b"FULLRESYNC"
        self.replication_id = id
        self.replication_offset = int(offset)

        # there's no specification on when the server will send the rdb file
        # after sending the FULLRESYNC command. It can be sent together in a
        # single write() call, or sent separately.

        # check if there's any remaining data. if so, parse it.
        # else, read again from the socket
        # for now, assume that the whole rdb file fits to the buffer (1024 bytes)
        # and can be read using a single read() call

        logger.info(f"rdb file request: {remain!r}")
        data = remain if remain else await reader.read(1024)
        rdb_file, _length, remain = RespParser.parse_rdb(data)
        logger.info(f"rdb file: {rdb_file!r}")

        # process requests that comes together with the rdb file, if any
        if remain:
            await self._parse_and_handle_request(remain, reader, writer)

    async def listen_to_master(self):
        reader, writer = self.master
        while True:
            data: bytes = await reader.read(1024)
            addr: str = writer.get_extra_info("peername")

            if not data:
                logger.info(f"closing connection with {addr}")
                writer.close()
                await writer.wait_closed()
                return
            await self._parse_and_handle_request(data, reader, writer)

    def _handle_info(self, req: List[bytes]) -> bytes:
        return super()._handle_info(b"role:slave", req)

    async def handle_master_request(
        self,
        req: List[bytes],
    ) -> bytes:
        assert len(req) > 0
        match req[0].upper():
            case b"SET":
                if len(req) < 3:
                    return b"-Missing argument(s) for SET\r\n"
                await self._handle_set(req)
                return b""
            case b"REPLCONF":
                if len(req) < 2:
                    return b"-Missing argument(s) for REPLCONF\r\n"
                return self._handle_replconf(req, self.master[0], self.master[1])
            case _:
                logger.error(f"Received {req[0]!r} command (not supported)!")
                return b""

    async def serve(self):
        async with self.server:
            await self.listen_to_master()
            await self.server.serve_forever()
