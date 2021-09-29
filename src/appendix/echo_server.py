import socket


def sync_main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 9000))
    sock.listen(1)

    conn, addr = sock.accept()
    while True:
        data = conn.recv(1024)
        str_data = data.decode("utf-8")
        print(f"received data: {str_data}")
        if not data:
            break
        conn.sendall(f"{str_data} - echo".encode())


if __name__ == "__main__":
    sync_main()
