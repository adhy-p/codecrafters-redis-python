import select
import socket
from enum import Enum

CR = b"\r\n"
RESP_TYPE = Enum("RESP_TYPE",
                 ["SIMPLESTRING", "ERROR", "INTEGER", "BULKSTRING", "ARRAY"])
RESP_SYMBOL = {
        RESP_TYPE.SIMPLESTRING: b"+",
        RESP_TYPE.ERROR: b"-",
        RESP_TYPE.INTEGER: b":",
        RESP_TYPE.BULKSTRING: b"$",
        RESP_TYPE.ARRAY: b"*",
        }


class RedisServer:
    def __init__(self):
        print("Initializing Redis Server")
        self.server_socket = socket.create_server(
                ("localhost", 6379), reuse_port=True)
        self.server_socket.setblocking(0)
        self.active_connections = [self.server_socket]

    def serve(self):
        while True:
            print("polling sockets")
            readable, writable, exceptional = select.select(
                    self.active_connections, [], [])

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

    def _handle_client(self, client):
        data = client.recv(2048)
        if data:
            print(f"received request from client: {data}")
            req = self._parse_request(data)
            self._handle_request(client, req)
        else:
            print("client disconnected")
            self.active_connections.remove(client)
            client.close()

    def _parse_request(self, req):
        print("parsing request: ", req)
        if req[:1] == RESP_SYMBOL[RESP_TYPE.SIMPLESTRING]:
            _, data = self._parse_simplestr(req)
            return data
        if req[:1] == RESP_SYMBOL[RESP_TYPE.BULKSTRING]:
            _, data = self._parse_bulkstr(req)
            return data
        if req[:1] == RESP_SYMBOL[RESP_TYPE.ARRAY]:
            _, data = self._parse_array(req)
            return data
        return b"TODO"

    def _parse_simplestr(self, data):
        assert (data[:1] == RESP_SYMBOL[RESP_TYPE.SIMPLESTRING])
        # data without type byte and <CR>
        return len(data), data[1:-2]

    def _parse_bulkstr(self, data):
        assert (data[:1] == RESP_SYMBOL[RESP_TYPE.BULKSTRING])
        next_cr = data.find(b'\r\n')
        str_len = int(data[1:next_cr].decode())
        # metadata: type, len, \r\n
        # actual data: 5th bytes onwards
        next_data = next_cr + 2
        return (next_data + str_len + 2), data[next_data:next_data+str_len]

    def _parse_array(self, data):
        assert (data[:1] == RESP_SYMBOL[RESP_TYPE.ARRAY])
        print("parsing array")
        next_cr = data.find(b'\r\n')
        arr_len = int(data[1:next_cr].decode())
        arr = []
        remaining_data = data[next_cr+2:]
        for i in range(arr_len):
            print("remaining_data", remaining_data)
            if remaining_data[:1] == RESP_SYMBOL[RESP_TYPE.SIMPLESTRING]:
                next_idx, element = self._parse_simplestr(remaining_data)
            if remaining_data[:1] == RESP_SYMBOL[RESP_TYPE.BULKSTRING]:
                next_idx, element = self._parse_bulkstr(remaining_data)
            if remaining_data[:1] == RESP_SYMBOL[RESP_TYPE.ARRAY]:
                next_idx, element = self._parse_array(remaining_data)
            arr.append(element)
            remaining_data = remaining_data[next_idx:]
        return next_idx, arr

    def _handle_request(self, client, req):
        print("handling request")
        print("parsed request:", req)
        assert (len(req) >= 1)

        req_type = req[0]
        resp_type = RESP_TYPE.SIMPLESTRING
        resp_body = b"UNKNOWN COMMAND"

        if req_type == b"PING":
            if len(req) == 1:
                resp_type = RESP_TYPE.SIMPLESTRING
                resp_body = b"PONG"
            else:
                resp_type = RESP_TYPE.BULKSTRING
                resp_body = req[1]
        elif req_type == b"ECHO":
            resp_type = RESP_TYPE.BULKSTRING
            resp_body = req[1]

        print("sending response to client")
        client.send(self._wrap_resp(resp_type, resp_body))

    def _wrap_resp(self, type_str, message):
        if type_str == RESP_TYPE.SIMPLESTRING:
            return RESP_SYMBOL[type_str] + message + CR
        if type_str == RESP_TYPE.BULKSTRING:
            return RESP_SYMBOL[type_str] \
                    + str(len(message)).encode("utf-8") + CR \
                    + message + CR
        if type_str == RESP_TYPE.ARRAY:
            return RESP_SYMBOL[type_str] + message + CR


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    # print("Logs from your program will appear here!")
    rs = RedisServer()
    rs.serve()


if __name__ == "__main__":
    main()
