from __future__ import annotations

import asyncio
from typing import Any, Mapping

from ..session import BlackcatSession
from ..typing import PlannerQueueEntry, Scheduler

RUN_PLANNER = object()


class AsyncIOScheduler(Scheduler):
    planner_queue: asyncio.Queue[PlannerQueueEntry]

    def __init__(self, num_workers: int = 1):
        # TODO: num_workersを使ってない
        self.planner_queue = asyncio.Queue()

    async def dispatch(self, session: BlackcatSession, planner: str, args: Mapping[str, Any]) -> None:
        await self.planner_queue.put((session, planner, args))

    async def run(self) -> None:
        """
        asyncio.Queueで順番に処理する

        Queueが空になるまで回す
        """
        while not self.planner_queue.empty():
            session, planner, args = self.planner_queue.get_nowait()
            await session.handle_planner(planner, args)
