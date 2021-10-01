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


class ComponentExecutor:
    _component: Component
    _instance_executors: list["InstanceExecutor"]

    def __init__(self, component: Component):
        super().__init__()
        self._component = component
        self._instance_executors = []

    def get_component(self) -> Component:
        return self._component

    def get_instance_executors(self) -> list["InstanceExecutor"]:
        return self._instance_executors

    def set_incoming_queues(self, queues: list[trio.MemoryReceiveChannel]) -> None:
        for idx, queue in enumerate(queues):
            self._instance_executors[idx].set_incoming_queue(queue)

    def set_outgoing_queue(self, queue: trio.MemorySendChannel) -> None:
        for executor in self._instance_executors:
            executor.set_outgoing_queue(queue)


class InstanceExecutor(Process):
    _event_collector: list[Event]
    _incoming_queue: trio.MemoryReceiveChannel = None
    _outgoing_queue: trio.MemorySendChannel = None

    def __init__(self):
        self._event_collector = []

    def set_incoming_queue(self, queue: trio.MemoryReceiveChannel) -> None:
        self._incoming_queue = queue

    def set_outgoing_queue(self, queue: trio.MemorySendChannel) -> None:
        self._outgoing_queue = queue


class OperatorInstanceExecutor(InstanceExecutor):
    _instance_id: int
    _operator: Operator

    def __init__(self, instance_id: int, operator: Operator):
        super().__init__()
        self._instance_id = instance_id
        self._operator = operator

    @classmethod
    async def create(cls, instance_id: int, operator: Operator):
        await operator.setup_instance(instance_id)
        return cls(instance_id, operator)

    def __repr__(self):
        return f"source: {self._operator.get_name()}, instance: {self._instance_id}"

    def _shutdown(self):
        self._incoming_queue.close()
        if self._outgoing_queue is not None:
            self._outgoing_queue.close()

    async def run_once(self) -> bool:
        try:
            event = await self._incoming_queue.receive()
            self._operator.apply(event, self._event_collector)
            if self._outgoing_queue is not None:
                for output in self._event_collector:
                    await self._outgoing_queue.send(output)
            self._event_collector.clear()
        except trio.EndOfChannel:
            print(f"Incoming channel closed. {self} is shutting down.")
            self._shutdown()
            return False
        except Exception as e:
            self._shutdown()
            raise e

        return True


class OperatorExecutor(ComponentExecutor):
    _operator: Operator

    def __init__(
        self, operator: Operator, list_executors: list[OperatorInstanceExecutor]
    ):
        super().__init__(operator)
        self._operator = operator
        self._instance_executors = list_executors

    @classmethod
    async def create(cls, operator: Operator):
        instance_executors = []
        for idx in range(operator.get_parallelism()):
            clone = operator.clone()
            executor = await OperatorInstanceExecutor.create(idx, clone)
            instance_executors.append(executor)
        return cls(operator, instance_executors)

    def get_grouping_strategy(self) -> GroupingStrategy:
        return self._operator.get_grouping_strategy()

    async def run(self, nursery: trio.Nursery):
        for instance in self._instance_executors:
            nursery.start_soon(instance.run)


class SourceInstanceExecutor(InstanceExecutor):
    _instance_id: int
    _source: Source

    def __init__(self, instance_id: int, src: Source):
        super().__init__()
        self._source = src
        self._instance_id = instance_id

    @classmethod
    async def create(cls, instance_id: int, src: Source):
        await src.setup_instance(instance_id)
        return cls(instance_id, src)

    def __repr__(self):
        return f"source: {self._source.get_name()}, instance: {self._instance_id}"

    def _shutdown(self):
        # TODO: hmm do we need to close all the outgoing queues, or will the others take care of themselves?
        self._outgoing_queue.close()

    async def run_once(self) -> bool:
        try:
            await self._source.get_events(self._event_collector)
            for event in self._event_collector:
                await self._outgoing_queue.send(event)
            self._event_collector.clear()
        except trio.BrokenResourceError as e:
            print(str(e))
            print(f"Incoming channel closed. {self} is shutting down.")
            self._shutdown()
            return False
        except Exception as e:
            self._shutdown()
            raise e

        return True


class SourceExecutor(ComponentExecutor):
    _source: Source

    def __init__(self, src: Source, list_executors: list[SourceInstanceExecutor]):
        super().__init__(src)
        self._source = src
        self._instance_executors = list_executors

    @classmethod
    async def create(cls, src: Source):
        instance_executors = []
        for idx in range(src.get_parallelism()):
            clone = src.clone()
            executor = await SourceInstanceExecutor.create(idx, clone)
            instance_executors.append(executor)
        return cls(src, instance_executors)

    async def run(self, nursery: trio.Nursery):
        for instance in self._instance_executors:
            nursery.start_soon(instance.run)

    def set_incoming_queues(self, queue: trio.MemoryReceiveChannel) -> None:
        raise RuntimeError("no incoming queue allowed for a source executor")


class Connection:
    from_: ComponentExecutor
    to_: OperatorExecutor

    def __init__(self, from_: ComponentExecutor, to_: OperatorExecutor):
        self.from_ = from_
        self.to_ = to_


class EventDispatcher(Process):
    _downstream_executor: OperatorExecutor
    _incoming_queue: trio.MemoryReceiveChannel
    _outgoing_queues: list[trio.MemorySendChannel]

    def __init__(self, op_executor: OperatorExecutor):
        self._downstream_executor = op_executor
        self._incoming_queue = None
        self._outgoing_queues = []

    def set_incoming_queue(self, queue: trio.MemoryReceiveChannel):
        self._incoming_queue = queue

    def set_outgoing_queues(self, queues: list[trio.MemorySendChannel]):
        self._outgoing_queues = queues

    async def run_once(self) -> bool:
        try:
            event = await self._incoming_queue.receive()
            grouping = self._downstream_executor.get_grouping_strategy()
            instance = grouping.get_instance(event, len(self._outgoing_queues))
            await self._outgoing_queues[instance].send(event)
        except trio.EndOfChannel:
            print(f"Incoming channel closed. Event dispatcher is shutting down.")
            return False

        return True


class JobStarter:
    _queue_size: int = 64
    _job: Job
    _executor_list: list[ComponentExecutor]
    _dispatcher_list: list[EventDispatcher]
    _connection_list: list[Connection]

    def __init__(self, job: Job):
        self._job = job
        self._connection_list = []
        self._dispatcher_list = []
        self._executor_list = []

    async def setup(self):
        await self.setup_component_executors()
        self.setup_connections()

    async def setup_component_executors(self):
        for src in self._job.get_sources():
            executor = await SourceExecutor.create(src)
            self._executor_list.append(executor)
            await self.traverse_component(src, executor)

    def setup_connections(self):
        for conn in self._connection_list:
            self.connect_executors(conn)

    async def start(self, nursery: trio.Nursery):
        for executor in reversed(self._executor_list):
            nursery.start_soon(executor.run, nursery)
        for dispatcher in self._dispatcher_list:
            nursery.start_soon(dispatcher.run)

    def connect_executors(self, conn: Connection):
        """
        Multiple instances of both upstream and downstream components.
        1 dispatcher handles all instances of the upstream using 1 queue, and
        routes them to n queues per the grouping strategy for the downstream component,
        1 queue per instance.
        """
        dispatcher = EventDispatcher(conn.to_)
        self._dispatcher_list.append(dispatcher)

        upstream_send, upstream_recv = trio.open_memory_channel(self._queue_size)
        conn.from_.set_outgoing_queue(upstream_send)
        dispatcher.set_incoming_queue(upstream_recv)

        downstream_channels = [
            trio.open_memory_channel(self._queue_size)
            for _ in range(conn.to_.get_component().get_parallelism())
        ]
        dispatcher.set_outgoing_queues([channel[0] for channel in downstream_channels])
        conn.to_.set_incoming_queues([channel[1] for channel in downstream_channels])

    async def traverse_component(
        self, component: Component, comp_executor: ComponentExecutor
    ):
        stream = component.get_outgoing_stream()
        for operator in stream.get_applied_operators():
            op_executor = await OperatorExecutor.create(operator)
            self._executor_list.append(op_executor)
            self._connection_list.append(
                Connection(from_=comp_executor, to_=op_executor)
            )
            await self.traverse_component(operator, op_executor)
