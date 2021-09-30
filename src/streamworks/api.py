from abc import ABC, abstractmethod
from typing import Any, Iterable


class Event(ABC):
    @abstractmethod
    def get_data(self) -> Any:
        pass


class Component:
    _name: str
    _outgoing_stream: "Stream"

    def __init__(self, name: str):
        self._name = name
        self._outgoing_stream = Stream()

    def get_name(self) -> str:
        return self._name

    def get_outgoing_stream(self) -> "Stream":
        return self._outgoing_stream


class Operator(Component):
    def __init__(self, name: str) -> None:
        super().__init__(name)

    @abstractmethod
    def apply(self, event: Event, event_collector: list[Event]):
        pass


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
    def __init__(self, name: str) -> None:
        super().__init__(name)

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
