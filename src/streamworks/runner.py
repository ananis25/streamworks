from datetime import datetime
from dataclasses import dataclass
import trio
import fire
import colorama

from .api import *
from .engine import *
from .jobs import *


async def windowing_test_job():
    FIXED_WINDOW_INTERVAL = 5000
    FIXED_WINDOW_WATERMARK = 2000

    job = Job("window-test-job")
    job.add_source(TransactionSource("trx-source", 1, 9000)).with_windowing(
        FixedTimeWindowStrategy(FIXED_WINDOW_INTERVAL, FIXED_WINDOW_WATERMARK)
    ).apply_operator(
        TestWindowedAnalyzer("test-analyzer", 1, UserAccountFieldsGrouping())
    )

    print(
        """This is a streaming job with a windowed strategy and a windowed operator. 
        Input transactions in the format: {amount},{time offset}. \n
        """
    )

    starter = JobStarter(job)
    await starter.setup()
    async with trio.open_nursery() as nursery:
        await starter.start(nursery)


def main(func_to_run: str):
    if func_to_run == "window_test":
        trio.run(windowing_test_job)
    else:
        pass


if __name__ == "__main__":
    colorama.init()
    fire.Fire(main)
