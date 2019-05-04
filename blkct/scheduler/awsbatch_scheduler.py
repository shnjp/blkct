from __future__ import annotations

from hashlib import md5
from typing import Any, Dict, List, Mapping

import boto3

from ..logging import logger
from ..session import BlackcatSession
from ..typing import PlannerQueueEntry, Scheduler
from ..utils import dump_json

batch_client = boto3.client("batch")

# dispatchをプロセス内で処理するならtrue
OPTIONS_IN_PROCESS = "in_process"


class AWSBatchScheduler(Scheduler):
    first_plan_dispatched: bool
    plans: List[PlannerQueueEntry]

    def __init__(self, job_definition: str, job_queue: str):
        if not job_definition:
            raise ValueError("job_definition required")
        if not job_queue:
            raise ValueError("job_queue required")
        self.job_definition = job_definition
        self.job_queue = job_queue
        self.first_plan_dispatched = False
        self.plans = []

    # override
    async def dispatch(
        self, session: BlackcatSession, planner: str, args: Mapping[str, Any], options: Dict[str, Any]
    ) -> None:
        if not self.first_plan_dispatched:
            # 最初のJobはこのプロセス内で処理するJob
            self.first_plan_dispatched = True
            self.plans.append((session, planner, args))
            return

        logger.info("Dispatch job", planner=planner, args=args)

        if options.get(OPTIONS_IN_PROCESS):
            self.plans.append((session, planner, args))
        else:
            # TODO:blockしている
            logger.info("Submit job", planner=planner, args=args)
            json_data = dump_json(args)
            response = batch_client.submit_job(
                jobName=f"{session.session_id}_{planner}_{md5(json_data.encode('utf-8')).hexdigest()}",
                jobQueue=self.job_queue,
                jobDefinition=self.job_definition,
                parameters={"planner": planner, "args": json_data},
                containerOverrides={"environment": [{"name": "BLKCT_SESSION_ID", "value": session.session_id}]},
            )
            job_id = response["jobId"]
            logger.info("Job submitted", job_id=job_id)

    async def run(self) -> None:
        if not self.plans:
            raise Exception("No plans dispatched")

        # submit other jobs
        while self.plans:
            session, planner, args = self.plans.pop(0)
            await session.handle_planner(planner, args)
