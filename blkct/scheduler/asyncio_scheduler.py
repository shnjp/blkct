from __future__ import annotations

import asyncio
from typing import Dict, Tuple

from ..session import BlackcatSession
from ..typing import Scheduler

PlannerQueueEntry = Tuple[BlackcatSession, str, Dict[str, str]]
RUN_PLANNER = object()


class AsyncIOScheduler(Scheduler):
    planner_queue: asyncio.Queue[PlannerQueueEntry]

    def __init__(self, num_workers: int = 1):
        self.planner_queue = asyncio.Queue()

    async def dispatch(self, session: BlackcatSession, planner: str, args: Dict[str, str]) -> None:
        await self.planner_queue.put((session, planner, args))

    async def run(self) -> None:
        while not self.planner_queue.empty():
            await self.run_once()

    async def run_once(self) -> None:
        """1 planner分走る"""
        session, planner, args = self.planner_queue.get_nowait()
        await session.handle_planner(planner, args)

    async def handle_planner(self, session: BlackcatSession, planner: str, args: Dict[str, str]) -> None:
        await session.handle_planner(planner, args)
