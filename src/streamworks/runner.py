from datetime import datetime
from dataclasses import dataclass
import trio
import fire

from .api import *
from .engine import *
from .jobs import *


async def system_usage():
    job = Job("system-usage")

    job.add_source(TransactionSource("transaction-source", 1, 9000)).apply_operator(
        SystemUsageAnalyzer("system-usage-analyzer", 2, TransactionFieldsGrouping())
    ).apply_operator(UsageWriter("usage-writer", 2, TransactionFieldsGrouping()))

    print(
        """This is a streaming job to detect suspiscious transactions. 
        Input should be in the form: {amount},{merchandise_id}, like 43.2,24"""
    )
    starter = JobStarter(job)
    await starter.setup()
    async with trio.open_nursery() as nursery:
        await starter.start(nursery)


def main(func_to_run: str):
    if func_to_run == "system_usage":
        trio.run(system_usage)
    else:
        pass


if __name__ == "__main__":
    fire.Fire(main)
