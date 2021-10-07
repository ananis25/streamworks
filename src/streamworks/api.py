import time
import math
from abc import ABC, abstractmethod
from typing import Any, Iterable, Protocol, runtime_checkable, Union


TYPE_GROUPING_MAP = dict[str, "GroupingStrategy"]


@runtime_checkable
class Event(Protocol):
    pass


@runtime_checkable
class TimedEvent(Protocol):
    def get_time(self) -> int:
        pass


@runtime_checkable
class State(Protocol):
    pass


@runtime_checkable
class Stateful(Protocol):
    def setup_instance(self, state: State):
        pass

    def get_state(self) -> State:
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


class EventWindow:
    _events: list[Event]
    _start_time: int
    _end_time: int

    def __init__(self, start_time: int, end_time: int):
        self._start_time = start_time
        self._end_time = end_time
        self._events = []

    def get_start_time(self):
        return self._start_time

    def get_end_time(self):
        return self._end_time

    def add(self, event: Event):
        self._events.append(event)

    def get_events(self):
        return self._events


class Operator(Component):
    grouping_map: TYPE_GROUPING_MAP

    def __init__(
        self,
        name: str,
        parallelism: int,
        grouping: Union["GroupingStrategy", TYPE_GROUPING_MAP] = None,
    ) -> None:
        super().__init__(name, parallelism)
        if isinstance(grouping, dict):
            self.grouping_map = grouping
        elif grouping is not None:
            self.grouping_map = {"default": grouping}
        else:
            self.grouping_map = {"default": ShuffleGrouping()}

    @abstractmethod
    def clone(self) -> "Operator":
        raise NotImplementedError

    @abstractmethod
    async def setup_instance(self, instance: int):
        pass

    @abstractmethod
    def apply(self, event: Event, event_collector: list[Event]):
        pass

    def get_grouping_strategy(self, stream_name: str) -> "GroupingStrategy":
        return self.grouping_map[stream_name]

    def get_grouping_strategy_map(self) -> TYPE_GROUPING_MAP:
        return self.grouping_map


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

    def with_windowing(self, strategy: "WindowingStrategy"):
        return WindowedStream(self, strategy)

    def apply_window_operator(
        self, strategy: "WindowingStrategy", operator: "WindowOperator"
    ) -> "Stream":
        windowing_op = WindowingOperator(
            operator.get_name(),
            operator.get_parallelism(),
            operator.get_grouping_strategy(),
            strategy,
            operator,
        )
        self.apply_operator(windowing_op)
        return windowing_op.get_outgoing_stream()


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


@runtime_checkable
class GroupingStrategy(Protocol):
    ALL_INSTANCES: int = -1

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


class AllGrouping:
    def get_instance(self, event: Event, parallelism: int) -> int:
        return GroupingStrategy.ALL_INSTANCES


@runtime_checkable
class WindowingStrategy(Protocol):
    def add(self, event: Event, processing_time: int):
        pass

    def get_event_windows(self, processing_time: int) -> list:
        return self._list_windows

    def clone(self) -> "WindowingStrategy":
        pass


class SlidingTimeWindowStrategy:
    _length_ms: int
    _interval_ms: int  # interval bw adjacent windows
    _watermark_ms: int  # extra wait time before closing window
    _event_windows: dict[int, EventWindow]

    def __init__(self, length_ms: int, interval_ms: int, watermark_ms: int):
        self._length_ms = length_ms
        self._interval_ms = interval_ms
        self._watermark_ms = watermark_ms
        self._event_windows = dict()

    def clone(self):
        return self.__class__(self._length_ms, self._interval_ms, self._watermark_ms)

    def is_late_event(self, event_time: int, processing_time: int):
        return (processing_time - event_time) > (self._length_ms + self._watermark_ms)

    def add(self, event: Event, processing_time: int):
        assert issubclass(event.__class__, TimedEvent)
        event_time = event.get_time()
        if not self.is_late_event(event_time, processing_time):
            latest_start_time = (
                math.floor(event_time / self._interval_ms) * self._interval_ms
            )

            start_time = latest_start_time
            while event_time < start_time + self._length_ms:
                window = self._event_windows.get(start_time, None)
                if window is None:
                    window = EventWindow(start_time, start_time + self._length_ms)
                    self._event_windows[start_time] = window
                window.add(event)
                start_time -= self._interval_ms

    def get_event_windows(self, processing_time: int):
        """fetch event windows that are ready to be processed"""
        to_process: list[EventWindow] = []
        for start_time, window in self._event_windows.items():
            if processing_time > (start_time + self._length_ms + self._watermark_ms):
                to_process.append(window)

        for window in to_process:
            self._event_windows.pop(window.get_start_time())
        return to_process


class FixedTimeWindowStrategy(SlidingTimeWindowStrategy):
    """huh, this works neatly, but I gotta think about it"""

    def __init__(self, interval_ms: int, watermark_ms: int):
        super().__init__(interval_ms, interval_ms, watermark_ms)

    def clone(self):
        return self.__class__(self._interval_ms, self._watermark_ms)


class WindowedStream(Stream):
    base_stream: Stream
    strategy: WindowingStrategy

    def __init__(self, base_stream: Stream, strategy: WindowingStrategy):
        self.base_stream = base_stream
        self.strategy = strategy

    def apply_operator(self, operator: "WindowOperator"):
        return self.base_stream.apply_window_operator(self.strategy, operator)


class WindowOperator(Operator):
    def __init__(
        self, name: str, parallelism: int, grouping: "GroupingStrategy" = None
    ) -> None:
        super().__init__(name, parallelism, grouping)

    def apply(self, event: Event, event_collector: EventCollector):
        raise RuntimeError("unsupported for WindowOperator")

    @abstractmethod
    def apply(self, window: EventWindow, event_collector: EventCollector):
        raise NotImplementedError


class WindowingOperator(Operator):
    _strategy: "WindowingStrategy"
    _operator: "WindowOperator"

    def __init__(
        self,
        name: str,
        parallelism: int,
        grouping: GroupingStrategy,
        strategy: WindowingStrategy,
        operator: WindowOperator,
    ):
        super().__init__(name, parallelism, grouping)
        self._strategy = strategy
        self._operator = operator

    def clone(self):
        return self.__class__(
            self.get_name(),
            self.get_parallelism(),
            self.get_grouping_strategy(),
            self._strategy.clone(),
            self._operator.clone(),
        )

    async def setup_instance(self, instance: int):
        await self._operator.setup_instance(int)

    def apply(self, event: Event, event_collector: EventCollector):
        processing_time = int(time.time()) * 1000
        if event is not None:
            self._strategy.add(event, processing_time)

        for window in self._strategy.get_event_windows(processing_time):
            self._operator.apply(window, event_collector)


class JoinOperator(Operator):
    default_grouping: "GroupingStrategy"

    def __init__(self, name: str, parallelism: int, grouping_map: TYPE_GROUPING_MAP):
        super().__init__(name, parallelism, grouping_map)
        self.default_grouping = AllGrouping()

    def get_grouping_strategy(self, stream_name: str) -> "GroupingStrategy":
        return self.grouping_map.get(stream_name, self.default_grouping)


class NamedStreams:
    _streams: dict[Stream, str]

    def __init__(self, named_streams: dict[Stream, str]):
        self._streams = named_streams

    def join(self, operator: JoinOperator) -> Stream:
        for stream, name in self._streams.items():
            stream.apply_operator(operator, name)
        return operator.get_outgoing_stream()


class NameEventPair:
    _stream_name: str
    _event: Event

    def __init__(self, stream_name: str, event: Event):
        self._stream_name = stream_name
        self._event = event

    def get_stream_name(self):
        return self._stream_name

    def get_event(self):
        return self._event

    def __repr__(self):
        return f"[stream name: {self._stream_name}, event: {self._event}]"
