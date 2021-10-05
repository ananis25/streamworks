import uuid
import warnings
from datetime import datetime
from dataclasses import dataclass
import trio

from .api import *
from .engine import *


@dataclass
class TransactionEvent(Event):
    transaction_id: str
    amount: float
    transaction_time: datetime
    merchandise_id: int
    user_account: int

    def __post_init__(self):
        super().__init__()

    def __repr__(self):
        return f"[transaction:{self.transaction_id}, amount:{self.amount}, time:{self.transaction_time.strftime('%Y-%m-%d %H:%M:%S')}, merchandise: {self.merchandise_id}, user: {self.user_account}]"


@dataclass
class TransactionScoreEvent(Event):
    transaction: TransactionEvent
    score: float


class ScoreAggregator(Operator):
    _instance_id: int
    _store: "ScoreStorage"

    def __init__(
        self,
        name: str,
        parallelism: int,
        grouping: GroupingStrategy,
        store: "ScoreStorage",
    ):
        super().__init__(name, parallelism, grouping)
        self._store = store

    async def setup_instance(self, instance_id: int):
        self._instance_id = instance_id

    def clone(self):
        return self.__class__(
            self.get_name(),
            self.get_parallelism(),
            self.get_grouping_strategy(),
            ScoreStorage(),
        )

    def apply(self, trx: TransactionScoreEvent, event_collector: EventCollector):
        assert isinstance(trx, TransactionScoreEvent)
        old_score = self._store.get(trx.transaction.transaction_id, 0)
        self._store.put(trx.transaction.transaction_id, old_score + trx.score)


class ScoreStorage:
    trx_scores: dict[str, float]

    def __init__(self):
        self.trx_scores = dict()

    def get(self, trx: str, default_val: float):
        return self.trx_scores.get(trx, default_val)

    def put(self, trx: str, value: float):
        print(f"Transaction score change: {trx} ==> {value}")
        self.trx_scores[trx] = value


class TransactionFieldsGrouping(FieldsGrouping):
    def get_key(self, event: TransactionScoreEvent):
        if isinstance(event, TransactionEvent):
            event = TransactionScoreEvent(event, 0)
        else:
            assert isinstance(event, TransactionScoreEvent)
        return event.transaction.transaction_id


class UserAccountFieldsGrouping(FieldsGrouping):
    def get_key(self, event: TransactionEvent):
        assert isinstance(event, TransactionEvent)
        return event.transaction_id


class TransactionSource(Source):
    _instance_id: int
    base_port: int
    stream: trio.SocketStream

    def __init__(self, name: str, parallelism: int, base_port: int):
        super().__init__(name, parallelism)
        self.base_port = base_port

    def clone(self):
        return self.__class__(self.get_name(), self.get_parallelism(), self.base_port)

    async def setup_instance(self, instance_id: int) -> None:
        self._instance_id = instance_id
        self.stream = await trio.open_tcp_stream(
            "127.0.0.1", self.base_port + instance_id
        )

    async def get_events(self, event_collector: EventCollector):
        data: bytes = await self.stream.receive_some()
        if data is None or len(data) == 0:
            raise trio.BrokenResourceError("\nTCP socket connection is closed")

        trx = data.decode("utf-8").strip()
        try:
            values = trx.split(",")
            amount = float(values[0])
            merchandise_id = int(values[1])
        except:
            warnings.warn(f"Invalid input transaction: {trx}", RuntimeWarning)
            return

        user_account = 1
        trx_id = uuid.uuid4().hex
        trx_time = datetime.now()
        event = TransactionEvent(trx_id, amount, trx_time, merchandise_id, user_account)
        event_collector.add("default", event)

        print(
            f"\nTransaction ({self.get_name()}) :: instance {self._instance_id} ---> {event}"
        )


class SystemUsageAnalyzer(Operator):
    _instance_id: int
    _trx_count: int
    _fraud_trx_count: int

    def __init__(self, name: str, parallelism: int, grouping: GroupingStrategy):
        super().__init__(name, parallelism, grouping)
        self._trx_count = 0
        self._fraud_trx_count = 0

    async def setup_instance(self, instance_id: int):
        self._instance_id = instance_id

    def clone(self):
        return self.__class__(
            self.get_name(), self.get_parallelism(), self.get_grouping_strategy()
        )

    def apply(self, event: Event, event_collector: EventCollector):
        self._trx_count += 1
        event_collector.add(
            "default", UsageEvent(self._trx_count, self._fraud_trx_count)
        )


class UsageEvent(Event):
    trx_count: int
    fraud_trx_count: int

    def __init__(self, trx_count: int, fraud_trx_count: int):
        self.trx_count = trx_count
        self.fraud_trx_count = fraud_trx_count

    def __repr__(self):
        return f"[transaction count: {self.trx_count}, fraud transaction count: {self.fraud_trx_count}]"


class UsageWriter(Operator):
    _instance_id: int

    def __init__(self, name: str, parallelism: int, grouping: GroupingStrategy):
        return super().__init__(name, parallelism, grouping)

    async def setup_instance(self, instance_id: int):
        self._instance_id = instance_id

    def clone(self):
        return self.__class__(
            self.get_name(), self.get_parallelism(), self.get_grouping_strategy()
        )

    def apply(self, score: Event, event_collector: EventCollector):
        assert isinstance(score, UsageEvent)
        print(score)
