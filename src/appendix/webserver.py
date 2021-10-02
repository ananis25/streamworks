from .engine import *


class Node:
    name: str

    def __init__(self, name: str):
        self.name = name


class Edge:
    from_: str
    to_: str

    def __init__(self, from_: Node, to_: Node):
        self.from_ = from_.name
        self.to_ = to_.name


class WebServer:
    job_name: str
    sources: list[Node]
    operators: list[Node]
    edges: list[Edge]

    def __init__(self, job_name: str, list_conns: list[Connection]):
        self.job_name = job_name

        incoming_counts: dict[Node, int] = dict()
        for conn in list_conns:
            from_ = Node(conn.from_.get_component().get_name())
            to_ = Node(conn.to_.get_component().get_name())

            # TODO: this looks off
            incoming_counts[from_] = incoming_counts.get(to_, 0)
            incoming_counts[to_] = incoming_counts.get(to_, 0) + 1

            self.edges.append(Edge(from_, to_))

        for node, val in incoming_counts.items():
            if val == 0:
                self.sources.append(node)
            else:
                self.operators.append(node)

    def start():
        pass
