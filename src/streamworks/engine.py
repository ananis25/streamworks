from abc import abstractmethod
import json

import trio
from hypercorn.config import Config
from hypercorn.trio import serve
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse, JSONResponse
from starlette.routing import Route

from .api import *

# alias the trio memory channel types so we don't conflate them with the concept of a `channel`
# in our streaming engine
SEND_QUEUE = trio.MemorySendChannel
RECV_QUEUE = trio.MemoryReceiveChannel


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

    def register_channel(self, channel: str):
        for executor in self._instance_executors:
            executor.register_channel(channel)

    def set_incoming_queues(self, queues: list[RECV_QUEUE]) -> None:
        for idx, queue in enumerate(queues):
            self._instance_executors[idx].set_incoming_queue(queue)

    def add_outgoing_queue(self, channel: str, queue: SEND_QUEUE) -> None:
        for executor in self._instance_executors:
            executor.add_outgoing_queue(channel, queue)


class InstanceExecutor(Process):
    _event_collector: EventCollector
    _incoming_queue: RECV_QUEUE
    _outgoing_queue_map: dict[str, list[SEND_QUEUE]]

    def __init__(self):
        self._event_collector = EventCollector()
        self._incoming_queue = None
        self._outgoing_queue_map = dict()

    def register_channel(self, channel: str):
        if channel not in self._outgoing_queue_map:
            self._outgoing_queue_map[channel] = []

    def set_incoming_queue(self, queue: RECV_QUEUE) -> None:
        self._incoming_queue = queue

    def add_outgoing_queue(self, channel: str, queue: SEND_QUEUE) -> None:
        if channel not in self._outgoing_queue_map:
            self.register_channel(channel)
        self._outgoing_queue_map[channel].append(queue)


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
        for _, queues in self._outgoing_queue_map.items():
            for queue in queues:
                queue.close()

    async def run_once(self) -> bool:
        try:
            event = await self._incoming_queue.receive()
            self._operator.apply(event, self._event_collector)

            for channel in self._event_collector.get_registered_channels():
                for output in self._event_collector.get_list_events(channel):
                    for queue in self._outgoing_queue_map[channel]:
                        await queue.send(output)
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

    async def run(self, nursery: trio.Nursery):
        for instance in self._instance_executors:
            nursery.start_soon(instance.run)

    def get_grouping_strategy(self) -> GroupingStrategy:
        return self._operator.get_grouping_strategy()


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
        for _, queues in self._outgoing_queue_map.items():
            for queue in queues:
                queue.close()

    async def run_once(self) -> bool:
        try:
            await self._source.get_events(self._event_collector)

            for channel in self._event_collector.get_registered_channels():
                for output in self._event_collector.get_list_events(channel):
                    for queue in self._outgoing_queue_map.get(channel, []):
                        await queue.send(output)
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

    def set_incoming_queues(self, queue: RECV_QUEUE) -> None:
        raise RuntimeError("no incoming queue allowed for a source executor")


class Connection:
    from_: ComponentExecutor
    to_: OperatorExecutor
    channel: str

    def __init__(self, from_: ComponentExecutor, to_: OperatorExecutor, channel: str):
        self.from_ = from_
        self.to_ = to_
        self.channel = channel


class EventDispatcher(Process):
    _downstream_executor: OperatorExecutor
    _incoming_queue: RECV_QUEUE
    _outgoing_queues: list[SEND_QUEUE]

    def __init__(self, op_executor: OperatorExecutor):
        self._downstream_executor = op_executor
        self._incoming_queue = None
        self._outgoing_queues = []

    def set_incoming_queue(self, queue: RECV_QUEUE):
        self._incoming_queue = queue

    def set_outgoing_queues(self, queues: list[SEND_QUEUE]):
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
    _operator_map: dict[Operator, OperatorExecutor]
    _operator_queue_map: dict[OperatorExecutor, SEND_QUEUE]  # TODO: check queue type
    _server: "WebServer"

    def __init__(self, job: Job):
        self._job = job
        self._connection_list = []
        self._dispatcher_list = []
        self._executor_list = []
        self._operator_map = dict()
        self._operator_queue_map = dict()
        self._server = WebServer(job.get_name())

    async def setup(self):
        await self.setup_component_executors()
        self.setup_connections()
        self._server.setup(self._connection_list)

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
        nursery.start_soon(
            serve, self._server, Config.from_mapping({"bind": "127.0.0.1:9010"})
        )

    def connect_executors(self, conn: Connection):
        """
        Multiple instances of both upstream and downstream components.
        1 dispatcher handles all instances of the upstream using 1 queue, and
        routes them to n queues per the grouping strategy for the downstream component,
        1 queue per instance.
        """
        conn.from_.register_channel(conn.channel)
        if conn.to_ in self._operator_queue_map:
            dispatch_queue = self._operator_queue_map[conn.to_]
            conn.from_.add_outgoing_queue(conn.channel, dispatch_queue)
        else:
            # upstream connections
            upstream_send, upstream_recv = trio.open_memory_channel(self._queue_size)
            conn.from_.add_outgoing_queue(conn.channel, upstream_send)
            dispatcher = EventDispatcher(conn.to_)
            dispatcher.set_incoming_queue(upstream_recv)
            self._operator_queue_map[conn.to_] = upstream_send

            # downstream connections
            downstream_queues = [
                trio.open_memory_channel(self._queue_size)
                for _ in range(conn.to_.get_component().get_parallelism())
            ]
            dispatcher.set_outgoing_queues(
                [channel[0] for channel in downstream_queues]
            )
            conn.to_.set_incoming_queues([channel[1] for channel in downstream_queues])

            # add to the list
            self._dispatcher_list.append(dispatcher)

    async def traverse_component(
        self, component: Component, comp_executor: ComponentExecutor
    ):
        stream = component.get_outgoing_stream()
        for channel in stream.get_channels():
            for operator in stream.get_applied_operators(channel):
                if operator in self._operator_map:
                    op_executor = self._operator_map[operator]
                else:
                    op_executor = await OperatorExecutor.create(operator)
                    self._operator_map[operator] = op_executor
                    self._executor_list.append(op_executor)
                    await self.traverse_component(operator, op_executor)

                self._connection_list.append(
                    Connection(from_=comp_executor, to_=op_executor, channel=channel)
                )


class Node:
    name: str
    parallelism: str

    def __init__(self, name: str, parallelism: int):
        self.name = name
        self.parallelism = str(parallelism)

    def get_json(self):
        return {"name": self.name, "parallelism": self.parallelism}


class Edge:
    from_: str
    to_: str
    stream: str
    from_parallelism: str
    to_parallelism: str

    def __init__(self, from_: Node, to_: Node, stream: str):
        self.from_ = from_.name
        self.to_ = to_.name
        self.stream = stream
        self.from_parallelism = from_.parallelism
        self.to_parallelism = to_.parallelism

    def get_json(self):
        return {
            "from": f"{self.from_} x{self.from_parallelism}",
            "to": f"{self.to_} x{self.to_parallelism}",
            "stream": self.stream,
        }


class WebServer(Starlette):
    job_name: str
    sources: list[Node]
    operators: list[Node]
    edges: list[Edge]

    def __init__(self, job_name: str):
        """thanks to https://github.com/encode/starlette/issues/811#issuecomment-900579064"""
        super().__init__(
            routes=[Route("/", self.index), Route("/plan.json", self.plan)]
        )
        self.job_name = job_name
        self.sources = []
        self.operators = []
        self.edges = []

    def setup(self, connections: list[Connection]):
        incoming_counts: dict[Node, int] = dict()
        for conn in connections:
            from_ = Node(
                conn.from_.get_component().get_name(),
                conn.from_.get_component().get_parallelism(),
            )
            to_ = Node(
                conn.to_.get_component().get_name(),
                conn.to_.get_component().get_parallelism(),
            )
            incoming_counts[from_] = incoming_counts.get(from_, 0)
            incoming_counts[to_] = incoming_counts.get(to_, 0) + 1
            self.edges.append(Edge(from_, to_, conn.channel))

        for node, count in incoming_counts.items():
            if count == 0:
                self.sources.append(node)
            else:
                self.operators.append(node)

    async def plan(self, request):
        plan = {
            "job": self.job_name,
            "sources": [s.get_json() for s in self.sources],
            "operators": [op.get_json() for op in self.operators],
            "edges": [edge.get_json() for edge in self.edges],
        }
        return PlainTextResponse(json.dumps(plan, indent=2), status_code=200)

    async def index(self, request):
        graph = [self.job_name]
        for edge in self.edges:
            graph.append(
                f"{edge.from_} x{edge.from_parallelism} --> | {edge.stream} | {edge.to_} x{edge.to_parallelism}"
            )
        return PlainTextResponse("\n".join(graph), status_code=200)
