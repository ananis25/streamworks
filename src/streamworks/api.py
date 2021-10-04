from abc import ABC, abstractmethod
from typing import Any, Iterable, Protocol


class Event(ABC):
    pass


class EventCollector:
    """
    Accept events from components, put them in the specified channel
    """

    _queues: dict[str, list[Event]]
    _channels_regd: set[str]

    def __init__(self):
        self._queues = dict()
        self._channels_regd = set()

    def register_channel(self, channel: str):
        # always make sure registration is idempotent
        if channel not in self._channels_regd:
            self._channels_regd.add(channel)
            self._queues[channel] = []

    def add(self, channel: str, event: Event):
        if channel not in self._queues:
            self.register_channel(channel)
        self._queues[channel].append(event)

    def get_registered_channels(self):
        return self._channels_regd

    def get_list_events(self, channel: str):
        return self._queues[channel]

    def clear(self):
        for queue in self._queues.values():
            queue.clear()


class Component:
    _name: str
    _outgoing_stream: "Stream"
    _parallelism: int

    def __init__(self, name: str, parallelism: int):
        self._name = name
        self._parallelism = parallelism
        self._outgoing_stream = Stream()

    def get_name(self) -> str:
        return self._name

    def get_outgoing_stream(self) -> "Stream":
        return self._outgoing_stream

    def get_parallelism(self) -> int:
        return self._parallelism

    @abstractmethod
    def clone(self) -> "Component":
        pass


class Operator(Component):
    grouping: "GroupingStrategy"

    def __init__(
        self, name: str, parallelism: int, grouping: "GroupingStrategy" = None
    ) -> None:
        super().__init__(name, parallelism)
        self.grouping = ShuffleGrouping() if not grouping else grouping

    @abstractmethod
    def clone(self) -> "Operator":
        raise NotImplementedError

    @abstractmethod
    async def setup_instance(self, instance: int):
        pass

    @abstractmethod
    def apply(self, event: Event, event_collector: list[Event]):
        pass

    def get_grouping_strategy(self) -> "GroupingStrategy":
        return self.grouping


class StreamChannel:
    base_stream: "Stream"
    channel: str

    def __init__(self, stream: str, channel: str):
        self.base_stream = stream
        self.channel = channel

    def apply_operator(self, operator: "Operator"):
        return self.base_stream.apply_operator(operator, channel=self.channel)


class Stream:
    _operator_map: dict[str, set[Operator]]

    def __init__(self):
        self._operator_map = dict()

    def apply_operator(
        self, operator: "Operator", channel: str = "default"
    ) -> "Stream":
        if channel not in self._operator_map:
            self._operator_map[channel] = set()

        op_set = self._operator_map[channel]
        if operator in op_set:
            raise RuntimeError(
                f"Operator {operator} already added to the channel: {channel}"
            )
        else:
            op_set.add(operator)
        return operator.get_outgoing_stream()

    def select_channel(self, channel: str):
        return StreamChannel(self, channel)

    def get_channels(self) -> list[str]:
        return list(self._operator_map.keys())

    def get_applied_operators(self, channel: str) -> Iterable[Operator]:
        return self._operator_map[channel]


class Streams:
    streams: list[Stream]

    def __init__(self, streams: list[Stream]):
        self.streams = streams

    @classmethod
    def of_(cls, streams: Iterable[Stream]):
        return cls([s for s in streams])

    def apply_operator(self, operator: Operator):
        for stream in self.streams:
            stream.apply_operator(operator)
        return operator.get_outgoing_stream()


class Source(Component):
    def __init__(self, name: str, parallelism: int) -> None:
        super().__init__(name, parallelism)

    @abstractmethod
    def clone(self) -> "Source":
        pass

    @abstractmethod
    async def setup_instance(self, instance: int):
        pass

    @abstractmethod
    async def get_events(self, event_collector: list[Event]):
        pass


class Job:
    _name: str
    _source_set: set[Source]

    def __init__(self, name: str):
        self._name = name
        self._source_set = set()

    def add_source(self, source: Source) -> Stream:
        if source in self._source_set:
            raise RuntimeError(f"Source {source} already added")

        self._source_set.add(source)
        return source.get_outgoing_stream()

    def get_name(self) -> str:
        return self._name

    def get_sources(self) -> Iterable[Source]:
        return self._source_set


class GroupingStrategy(Protocol):
    def get_instance(self, event: Event, parallelism: int) -> int:
        pass


class FieldsGrouping:
    @abstractmethod
    def get_key(self, event: Event) -> Any:
        raise NotImplementedError

    def get_instance(self, event: Event, parallelism: int) -> int:
        return abs(hash(self.get_key(event))) % parallelism


class ShuffleGrouping:
    count: int

    def __init__(self):
        self.count = 0

    def get_instance(self, event: Event, parallelism: int) -> int:
        if self.count >= parallelism:
            self.count = 0
        self.count += 1
        return self.count - 1
