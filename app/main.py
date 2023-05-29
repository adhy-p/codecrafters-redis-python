import select
import socket


def wrap_resp(message):
    first = b"+"
    end = b"\r\n"
    return first + message.encode("utf-8") + end


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.setblocking(0)

    active_connections = [server_socket]

    while True:
        print("polling sockets")
        readable, writable, exceptional = select.select(active_connections, [], [])

        for s in readable:
            if s is server_socket:
                conn, addr = server_socket.accept()  # wait for client
                print(f"received connection from {addr}")
                active_connections.append(conn)
                conn.recv(2048)
                conn.send(wrap_resp("PONG"))
            else:
                data = s.recv(2048)
                if data:
                    print("sending response to ping message")
                    s.send(wrap_resp("PONG"))
                else:
                    print("client disconnected")
                    active_connections.remove(s)
                    s.close()

        for s in exceptional:
            print("exception")
            active_connections.remove(s)
            s.close()


if __name__ == "__main__":
    main()
