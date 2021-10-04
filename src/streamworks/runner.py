from datetime import datetime
from dataclasses import dataclass
import trio
import fire

from .api import *
from .engine import *
from .jobs import *


async def fraud_detection():
    job = Job("fraud-detection")

    transaction_out = job.add_source(TransactionSource("transaction-source", 1, 9000))
    eval_results_1 = transaction_out.apply_operator(
        "default",
        AvgTicketAnalyzer("avg-ticket-analyzer", 2, UserAccountFieldsGrouping()),
    )
    eval_results_2 = transaction_out.apply_operator(
        "default",
        WindowedProximityAnalyzer(
            "window-proximity-analyzer", 2, UserAccountFieldsGrouping()
        ),
    )
    eval_results_3 = transaction_out.apply_operator(
        "default",
        WindowedTransactionCountAnalyzer(
            "window-count-analyzer", 2, UserAccountFieldsGrouping()
        ),
    )

    store = ScoreStorage()
    Streams.of_([eval_results_1, eval_results_2, eval_results_3]).apply_operator(
        ScoreAggregator("score-aggregator", 2, TransactionFieldsGrouping(), store)
    )
    print(
        """
    Streaming job that detects suspicious transactions. 
    
    Input needs to be in this format: {amount},{merchandiseId}. For example: 42.00,3\n
    """
    )

    starter = JobStarter(job)
    await starter.setup()
    async with trio.open_nursery() as nursery:
        await starter.start(nursery)


async def stream_fork():
    job = Job("stream-fork")

    stream = job.add_source(Bridge("bridge", 1, 9000, False))
    stream.apply_operator(
        "default", TollBooth("booth/shuffle grouping", 2, ShuffleGrouping())
    )
    stream.apply_operator(
        "default",
        TollBooth("booth clone/fields grouping", 2, VehicleTypeFieldsGrouping()),
    )

    print(
        """This is a streaming job with 2 counting operators attached to the same input stream 
        but different logic for grouping. 
        """
    )

    starter = JobStarter(job)
    await starter.setup()
    async with trio.open_nursery() as nursery:
        await starter.start(nursery)


async def stream_merge():
    job = Job("stream-merge")

    stream1 = job.add_source(Bridge("bridge-1", 1, 9000, False))
    stream2 = job.add_source(Bridge("bridge-2", 1, 9001, True))

    Streams.of_([stream1, stream2.select_channel("clone")]).apply_operator(
        TollBooth("booth", 2, VehicleTypeFieldsGrouping())
    )

    print(
        "This is a streaming job with 1 counting operator attached to two input streams\n"
    )

    starter = JobStarter(job)
    await starter.setup()
    async with trio.open_nursery() as nursery:
        await starter.start(nursery)


async def stream_split():
    job = Job("stream-split")

    stream = job.add_source(Bridge("bridge", 1, 9000, True))

    stream.apply_operator(TollBooth("booth/shuffle grouping", 2, ShuffleGrouping()))
    stream.select_channel("clone").apply_operator(
        TollBooth("booth clone/fields grouping", 2, VehicleTypeFieldsGrouping())
    )

    print(
        """This is a streaming job with 2 counting operators attached to different channels of the 
        same input stream. 
        """
    )
    starter = JobStarter(job)
    await starter.setup()
    async with trio.open_nursery() as nursery:
        await starter.start(nursery)


def main(func_to_run: str):
    if func_to_run == "fraud_detection":
        trio.run(fraud_detection)
    elif func_to_run == "stream_fork":
        trio.run(stream_fork)
    elif func_to_run == "stream_merge":
        trio.run(stream_merge)
    elif func_to_run == "stream_split":
        trio.run(stream_split)
    else:
        pass


if __name__ == "__main__":
    fire.Fire(main)
