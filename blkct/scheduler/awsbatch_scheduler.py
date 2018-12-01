from __future__ import annotations

from hashlib import md5
from typing import Any, List, Mapping

import boto3

from ..logging import logger
from ..session import BlackcatSession
from ..typing import PlannerQueueEntry, Scheduler
from ..utils import dump_json

batch_client = boto3.client('batch')


class AWSBatchScheduler(Scheduler):
    plans: List[PlannerQueueEntry]

    def __init__(self, job_definition: str, job_queue: str):
        if not job_definition:
            raise ValueError('job_definition required')
        if not job_queue:
            raise ValueError('job_queue required')
        self.job_definition = job_definition
        self.job_queue = job_queue
        self.plans = []

    # override
    async def dispatch(self, session: BlackcatSession, planner: str, args: Mapping[str, Any]) -> None:
        logger.info('dispatch job %s:%r', planner, args)
        self.plans.append((session, planner, args))

    async def run(self) -> None:
        if not self.plans:
            raise Exception('No plans dispatched')

        session, planner, args = self.plans.pop(0)
        await session.handle_planner(planner, args)

        # submit other jobs
        for session, planner, args in self.plans:
            # TODO:blockしている
            logger.info('submit job %s:%r', planner, args)
            json_data = dump_json(args)
            response = batch_client.submit_job(
                jobName=f"{session.session_id}_{planner}_{md5(json_data.encode('utf-8')).hexdigest()}",
                jobQueue=self.job_queue,
                jobDefinition=self.job_definition,
                parameters={
                    'planner': planner,
                    'args': json_data
                }
            )
            job_id = response['jobId']
            logger.info('  -> jobId: %s', job_id)
