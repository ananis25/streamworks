import fire

from .jobs import SensorReader


def sock_debugger():
    reader = SensorReader("sensor-reader", 9000)
    mock_collector = []
    while True:
        reader.get_events(mock_collector)


if __name__ == "__main__":
    fire.Fire(sock_debugger)
