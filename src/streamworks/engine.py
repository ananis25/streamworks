from abc import abstractmethod
import trio

from .api import *


class Process:
    async def run(self):
        while await self.run_once():
            await self.run_once()

    @abstractmethod
    async def run_once(self) -> bool:
        pass


class ComponentExecutor(Process):
    _component: Component
    _event_collector: list[Event]
    _incoming_queue: trio.MemoryReceiveChannel = None
    _outgoing_queue: trio.MemorySendChannel = None

    def __init__(self, component: Component):
        super().__init__()
        self._component = component
        self._event_collector = []

    def get_component(self) -> Component:
        return self._component

    def set_incoming_queue(self, queue: trio.MemoryReceiveChannel) -> None:
        self._incoming_queue = queue

    def set_outgoing_queue(self, queue: trio.MemorySendChannel) -> None:
        self._outgoing_queue = queue


class OperatorExecutor(ComponentExecutor):
    _operator: Operator

    def __init__(self, operator: Operator):
        super().__init__(operator)
        self._operator = operator

    async def run_once(self) -> bool:
        try:
            event = await self._incoming_queue.receive()
            self._operator.apply(event, self._event_collector)
            if self._outgoing_queue is not None:
                for output in self._event_collector:
                    await self._outgoing_queue.send(output)
            self._event_collector.clear()
        except trio.EndOfChannel:
            print(
                f"Incoming channel closed. Operator: {self.get_component().get_name()} is shutting down."
            )
            self._incoming_queue.close()
            if self._outgoing_queue is not None:
                self._outgoing_queue.close()
            return False
        except Exception as e:
            self._incoming_queue.close()
            if self._outgoing_queue is not None:
                self._outgoing_queue.close()
            raise e

        return True


class SourceExecutor(ComponentExecutor):
    _source: Source

    def __init__(self, src: Source):
        super().__init__(src)
        self._source = src

    async def run_once(self) -> bool:
        try:
            await self._source.get_events(self._event_collector)
            for event in self._event_collector:
                await self._outgoing_queue.send(event)
            self._event_collector.clear()
        except trio.BrokenResourceError as e:
            print(str(e))
            print(
                f"Incoming channel closed. Source: {self.get_component().get_name()} is shutting down."
            )
            self._outgoing_queue.close()
            return False
        except Exception as e:
            self._outgoing_queue.close()
            raise e

        return True

    def set_incoming_queue(self, queue: trio.MemoryReceiveChannel) -> None:
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

    def setup(self):
        self.setup_component_executors()
        self.setup_connections()

    def setup_component_executors(self):
        for src in self._job.get_sources():
            executor = SourceExecutor(src)
            self._executor_list.append(executor)
            self.traverse_component(src, executor)

    def setup_connections(self):
        for conn in self._connection_list:
            self.connect_executors(conn)

    async def start(self, nursery: trio.Nursery):
        for executor in reversed(self._executor_list):
            nursery.start_soon(executor.run)

    def connect_executors(self, conn: Connection):
        send_channel, recv_channel = trio.open_memory_channel(self._queue_size)
        conn.from_.set_outgoing_queue(send_channel)
        conn.to_.set_incoming_queue(recv_channel)

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
