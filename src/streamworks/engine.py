from abc import abstractmethod
from dataclasses import dataclass
from queue import Queue
from threading import Thread

from .api import *


class Process:
    _thread: Thread

    def __init__(self):
        self._thread = Thread(target=self._run, args=())

    def _run(self):
        while self.run_once():
            pass

    @abstractmethod
    def run_once(self) -> bool:
        pass

    def start(self):
        self._thread.start()


class EventQueue(Queue):
    serial_version_uid: int = 3673430816396878407


class ComponentExecutor(Process):
    _component: Component
    _event_collector: list[Event]
    _incoming_queue: EventQueue = None
    _outgoing_queue: EventQueue = None

    def __init__(self, component: Component):
        super().__init__()
        self._component = component
        self._event_collector = []

    def get_component(self) -> Component:
        return self._component

    def set_incoming_queue(self, queue: EventQueue) -> None:
        self._incoming_queue = queue

    def set_outgoing_queue(self, queue: EventQueue) -> None:
        self._outgoing_queue = queue


class OperatorExecutor(ComponentExecutor):
    _operator: Operator

    def __init__(self, operator: Operator):
        super().__init__(operator)
        self._operator = operator

    def run_once(self) -> bool:
        try:
            event = self._incoming_queue.get()
        except Exception as e:
            # TODO: find a better parallel to Java InterruptedException for python.
            # Python doesn't seem to have a simple mechanism to interrupt threads
            return False

        self._operator.apply(event, self._event_collector)

        try:
            for output in self._event_collector:
                self._outgoing_queue.put(output)
            self._event_collector.clear()
        except Exception as e:
            return False

        return True


class SourceExecutor(ComponentExecutor):
    _source: Source

    def __init__(self, src: Source):
        super().__init__(src)
        self._source = src

    def run_once(self) -> bool:
        try:
            self._source.get_events(self._event_collector)
        except Exception as e:
            # TODO: find a way to coordinate among threads in python
            return False

        try:
            for event in self._event_collector:
                self._outgoing_queue.put(event)
            self._event_collector.clear()
        except Exception as e:
            return False

        return True

    def set_incoming_queue(self, queue: EventQueue) -> None:
        raise RuntimeError("no incoming queue allowed for a source executor")


class Connection:
    from_: ComponentExecutor
    to_: OperatorExecutor

    def __init__(self, from_: ComponentExecutor, to_: OperatorExecutor):
        self.from_ = from_
        self.to_ = to_


class JobStarter:
    _queue_size: int = 64
    _job: Job
    _executor_list: list[ComponentExecutor]
    _connection_list: list[Connection]

    def __init__(self, job: Job):
        self._job = job
        self._connection_list = []
        self._executor_list = []

    def start(self):
        self.setup_component_executors()
        self.setup_connections()

        self.start_processes()

        # TODO: start web server

    def setup_component_executors(self):
        for src in self._job.get_sources():
            executor = SourceExecutor(src)
            self._executor_list.append(executor)
            self.traverse_component(src, executor)

    def setup_connections(self):
        for conn in self._connection_list:
            self.connect_executors(conn)

    def start_processes(self):
        for executor in reversed(self._executor_list):
            executor.start()

    def connect_executors(self, conn: Connection):
        inter_queue = EventQueue(self._queue_size)
        conn.from_.set_outgoing_queue(inter_queue)
        conn.to_.set_incoming_queue(inter_queue)

    def traverse_component(
        self, component: Component, comp_executor: ComponentExecutor
    ):
        stream = component.get_outgoing_stream()

        for operator in stream.get_applied_operators():
            op_executor = OperatorExecutor(operator)
            self._executor_list.append(op_executor)
            self._connection_list.append(
                Connection(from_=comp_executor, to_=op_executor)
            )
            self.traverse_component(operator, op_executor)
