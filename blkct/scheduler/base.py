class Scheduler:

    async def dispatch(self, planner, args):
        raise NotImplementedError

    async def run(self):
        raise NotImplementedError
