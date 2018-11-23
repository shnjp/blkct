import asyncio

# from ..logging import logger
from .base import Scheduler

RUN_PLANNER = object()


class AsyncIOScheduler(Scheduler):

    def __init__(self, num_workers=1):
        self.planner_queue = asyncio.Queue()

    async def dispatch(self, session, planner, args):
        await self.planner_queue.put((session, planner, args))

    async def run(self):
        while not self.planner_queue.empty():
            await self.run_once()

    async def run_once(self):
        """1 planner分走る"""
        session, planner, args = self.planner_queue.get_nowait()
        await session.handle_planner(planner, args)

    async def handle_planner(self, session, planner, args):
        await session.handle_planner(planner, args)
