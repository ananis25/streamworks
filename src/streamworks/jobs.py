import socket
import traceback
from typing import TextIO

import trio
import fire

from .api import *
from .engine import *


class SensorReader(Source):
    stream: trio.SocketStream

    def __init__(self, name: str, stream: trio.SocketStream):
        super().__init__(name)
        self.stream = stream

    @classmethod
    async def create(cls, name: str, port: int) -> "SensorReader":
        stream = await trio.open_tcp_stream("127.0.0.1", port)
        return cls(name, stream)

    async def get_events(self, event_collector: list[Event]):
        data: bytes = await self.stream.receive_some()
        if data is None or len(data) == 0:
            raise trio.BrokenResourceError("\nTCP socket connection is closed")

        vehicle = data.decode("utf-8").strip()
        event_collector.append(VehicleEvent(vehicle))
        print(f"\nSensorReader ---> {vehicle}")


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


async def runner():
    job = Job("vehicle_count")

    reader = await SensorReader.create("sensor-reader", 9000)
    bridge_stream = job.add_source(reader)
    counter = VehicleCounter("vehicle-counter")
    bridge_stream.apply_operator(counter)

    print(
        """
    This is a streaming job that counts the number of vehicles of each type.

    Enter the type of each new vechicle in the input terminal and check back here for the current counts.
    """
    )
    job_starter = JobStarter(job)
    job_starter.setup()

    async with trio.open_nursery() as nursery:
        await job_starter.start(nursery)


def main():
    trio.run(runner)


if __name__ == "__main__":
    fire.Fire(main)