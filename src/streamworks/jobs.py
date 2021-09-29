import socket
import traceback
from typing import TextIO

import fire

from .api import *
from .engine import *


class SensorReader(Source):
    reader: TextIO

    def __init__(self, name: str, port: int):
        super().__init__(name)
        self.reader = self._setup_socket_reader(port)

    def get_events(self, event_collector: list[Event]):
        try:
            vehicle = self.reader.readline().strip()
            if vehicle is None or vehicle == "":
                raise Exception("Server is closed")
            event_collector.append(VehicleEvent(vehicle))
            print("")
            print(f"SensorReader ---> {vehicle}")
        except Exception as e:
            print("Failed to read input")
            print(traceback.format_exc())
            raise e

    def _setup_socket_reader(self, port: int) -> TextIO:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", port))
            return sock.makefile("r")
        except Exception as e:
            print(traceback.format_exc())
            raise e


class VehicleEvent(Event):
    type_: str

    def __init__(self, type_: str):
        self.type_ = type_

    def get_data(self) -> str:
        return self.type_


class VehicleCounter(Operator):
    counts: dict[str, int]

    def __init__(self, name: str):
        super().__init__(name)
        self.counts = dict()

    def print_counts(self):
        print("VEHICLE COUNTER -->", end="  ")
        for vehicle, count in self.counts.items():
            print(f"{vehicle}: {count}", end=", ")
        print("", flush=True)

    def apply(self, event: Event, event_collector: list[Event]):
        vehicle = event.get_data()
        assert isinstance(vehicle, str), "Unexpected vehicle type encountered"

        self.counts[vehicle] = self.counts.get(vehicle, 0) + 1
        self.print_counts()


def runner():
    job = Job("vehicle_count")

    bridge_stream = job.add_source(SensorReader("sensor-reader", 9000))
    bridge_stream.apply_operator(VehicleCounter("vehicle-counter"))

    print(
        """
    This is a streaming job that counts the number of vehicles of each type.

    Enter the type of each new vechicle in the input terminal and check back here for the current counts.
    """
    )
    job_starter = JobStarter(job)
    job_starter.start()


if __name__ == "__main__":
    fire.Fire(runner)
