from abc import ABC, abstractmethod
from typing import Any, Iterable, Protocol


class Event(ABC):
    @abstractmethod
    def get_data(self) -> Any:
        pass


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
        pass

    @abstractmethod
    async def setup_instance(self, instance: int):
        pass

    @abstractmethod
    def apply(self, event: Event, event_collector: list[Event]):
        pass

    def get_grouping_strategy(self) -> "GroupingStrategy":
        return self.grouping


class Stream:
    # TODO: should this be a set? We need an order dont we?
    _operator_set: set[Operator]

    def __init__(self):
        self._operator_set = set()

    def apply_operator(self, operator: "Operator") -> "Stream":
        if operator in self._operator_set:
            raise RuntimeError(f"Operator {operator} already added to the job")

        self._operator_set.add(operator)
        return operator.get_outgoing_stream()

    def get_applied_operators(self) -> Iterable[Operator]:
        return self._operator_set


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
    def get_key(self, event: Event) -> Any:
        return event.get_data()

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
