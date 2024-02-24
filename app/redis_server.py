import socket
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("redis_server")


class RedisServer:
    def __init__(self):
        self.server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
        logger.info("creating server")
        self.conn, _ = self.server_socket.accept()
        logger.info("accepting connections")

    def serve(self):
        while True:
            b = self.conn.recv(1024)
            logger.info(f"received bytes from client: {b}")
            self.conn.send(b"+PONG\r\n")
            logger.info("replied PONG to client")
