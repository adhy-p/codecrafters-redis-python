import select
import socket


class RedisServer:
    def __init__(self):
        print("Initializing Redis Server")
        self.server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
        self.server_socket.setblocking(0)
        self.active_connections = [self.server_socket]

    def serve(self):
        while True:
            print("polling sockets")
            readable, writable, exceptional = select.select(self.active_connections, [], [])

            for s in readable:
                if s is self.server_socket:
                    self._handle_connection()
                else:
                    self._handle_client(s)

            for s in writable:
                pass

            for s in exceptional:
                self.active_connections.remove(s)
                s.close()

    def _handle_connection(self):
        conn, addr = self.server_socket.accept()
        print(f"received new connection from {addr}")
        self.active_connections.append(conn)
        self._handle_client(conn)

    def _handle_client(self, client):
        data = client.recv(2048)
        if data:
            print("sending response to client")
            client.send(self._wrap_resp("PONG"))
        else:
            print("client disconnected")
            self.active_connections.remove(client)
            client.close()

    def _wrap_resp(self, message):
        type_str = b"+"
        end = b"\r\n"
        return type_str + message.encode("utf-8") + end


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    # print("Logs from your program will appear here!")
    rs = RedisServer()
    rs.serve()


if __name__ == "__main__":
    main()
