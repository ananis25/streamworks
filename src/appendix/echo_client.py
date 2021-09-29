import sys
import socket
import trio


def sync_main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 9000))
    while True:
        data = input("enter string to echo: ").strip()
        sock.sendall(data.encode())
        resp = sock.recv(1024)
        if not resp:
            break
        print(resp.decode())


async def sender(stream):
    print("sender task started")
    while True:
        data = b"async is confusing"
        print("sender sending data")
        await stream.send_all(data)
        await trio.sleep(1)


async def receiver(stream):
    print("receiver task started")
    while True:
        data = await stream.receive_some()
        if not data:
            break
        print(f"received some data: {data.decode('utf-8')}")
    print("receiver: comms closed")
    sys.exit(0)


async def parent():
    stream = await trio.open_tcp_stream("127.0.0.1", 9000)
    async with stream:
        async with trio.open_nursery() as nursery:
            print("start sender")
            nursery.start_soon(sender, stream)
            print("start receiver")
            nursery.start_soon(receiver, stream)


def async_main():
    trio.run(parent)


if __name__ == "__main__":
    # sync_main()
    async_main()
