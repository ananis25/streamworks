import trio

from .api import *
from .engine import *


class SensorReader(Source):
    _instance_id: int
    base_port: int
    stream: trio.SocketStream

    def __init__(self, name: str, parallelism: int, base_port: int):
        super().__init__(name, parallelism)
        self.base_port = base_port

    def clone(self):
        return self.__class__(self._name, self._parallelism, self.base_port)

    async def setup_instance(self, instance_id: int) -> None:
        self._instance_id = instance_id
        self.stream = await trio.open_tcp_stream(
            "127.0.0.1", self.base_port + instance_id
        )

    async def get_events(self, event_collector: list[Event]):
        data: bytes = await self.stream.receive_some()
        if data is None or len(data) == 0:
            raise trio.BrokenResourceError("\nTCP socket connection is closed")

        vehicle = data.decode("utf-8").strip()
        event_collector.append(VehicleEvent(vehicle))
        print(f"\nSensorReader :: instance {self._instance_id} ---> {vehicle}")


class VehicleEvent(Event):
    type_: str

    def __init__(self, type_: str):
        self.type_ = type_

    def get_data(self) -> str:
        return self.type_


class VehicleCounter(Operator):
    _instance_id: int
    counts: dict[str, int]

    def __init__(self, name: str, parallelism: int, grouping: GroupingStrategy = None):
        super().__init__(name, parallelism, grouping)
        self.counts = dict()

    def clone(self):
        return self.__class__(self._name, self._parallelism, self.grouping)

    async def setup_instance(self, instance_id: int) -> None:
        self._instance_id = instance_id

    def print_counts(self):
        for vehicle, count in self.counts.items():
            print(f"{vehicle}: {count}", end=", ")
        print("", flush=True)

    def apply(self, event: VehicleEvent, event_collector: list[Event]):
        vehicle = event.get_data()
        assert isinstance(vehicle, str), "Unexpected vehicle type encountered"

        self.counts[vehicle] = self.counts.get(vehicle, 0) + 1
        print(f"VEHICLE COUNTER :: instance {self._instance_id} -->", end="  ")
        self.print_counts()


async def runner_1():
    job = Job("vehicle_count")

    reader = SensorReader("sensor-reader", 2, 9000)
    bridge_stream = job.add_source(reader)
    counter = VehicleCounter("vehicle-counter", 1)
    bridge_stream.apply_operator(counter)

    print(
        """
    This is a streaming job that counts the number of vehicles from 2 input streams. 

    Enter the type of each new vechicle in the input terminal and check back here for the current counts.
    """
    )
    job_starter = JobStarter(job)
    await job_starter.setup()

    async with trio.open_nursery() as nursery:
        await job_starter.start(nursery)


async def runner_2():
    job = Job("vehicle_count")

    reader = SensorReader("sensor-reader", 2, 9000)
    bridge_stream = job.add_source(reader)
    counter = VehicleCounter("vehicle-counter", 2)
    bridge_stream.apply_operator(counter)

    print(
        """
    This is a streaming job that counts the number of vehicles from 2 input streams, into 2 counters. 

    Enter the type of each new vechicle in the input terminal and check back here for the current counts.
    """
    )
    job_starter = JobStarter(job)
    await job_starter.setup()

    async with trio.open_nursery() as nursery:
        await job_starter.start(nursery)


async def runner_3():
    job = Job("vehicle_count")

    reader = SensorReader("sensor-reader", 2, 9000)
    bridge_stream = job.add_source(reader)
    counter = VehicleCounter("vehicle-counter", 2, FieldsGrouping())
    bridge_stream.apply_operator(counter)

    print(
        """
    This is a streaming job that counts the number of vehicles from 2 input streams, into 2 counters with Field grouping. 
    Events of the same type are always routed to the same counter. 

    Enter the type of each new vechicle in the input terminal and check back here for the current counts.
    """
    )
    job_starter = JobStarter(job)
    await job_starter.setup()

    async with trio.open_nursery() as nursery:
        await job_starter.start(nursery)


def main():
    trio.run(runner_3)


if __name__ == "__main__":
    main()
