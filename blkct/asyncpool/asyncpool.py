"""
This is a fork of CailDog's asyncpool
https://github.com/CaliDog/asyncpool
"""
from __future__ import annotations

import asyncio
import datetime
import logging
import types
from typing import Any, Awaitable, Callable, Generic, List, Optional, TYPE_CHECKING, Tuple, Type, TypeVar, Union, cast

from typing_extensions import Protocol

PRIORITY_TERMINATE = 32767


def utc_now() -> datetime.datetime:
    # utcnow returns a naive datetime, so we have to set the timezone manually <sigh>
    return datetime.datetime.now(datetime.timezone.utc)


class Terminator:
    pass


TWorkerResult = TypeVar('TWorkerResult')
TQueueValue = TypeVar('TQueueValue')
if TYPE_CHECKING:
    TWorkerArgs = Tuple[Optional[asyncio.Future[TWorkerResult]], Any, Any]
else:
    TWorkerArgs = Tuple[Optional[asyncio.Future], Any, Any]
TQueueItem = Union[TWorkerArgs, Terminator]
TPrioirty = Union[int, Tuple[int, int]]
TPrioirtyQueueValue = Tuple[TPrioirty, TQueueValue]


class PriorityAsyncPoolInterface(Protocol):

    async def push_with_priority(self, priority: int, *args: Any,
                                 **kwargs: Any) -> Optional[asyncio.Future[TWorkerResult]]:
        raise NotImplementedError


class AsyncPoolBase(Generic[TWorkerResult, TQueueValue]):
    _exceptions: bool
    _loop: asyncio.AbstractEventLoop
    _first_push_dt: Optional[datetime.datetime]
    _job_accept_duration: Optional[datetime.timedelta]
    _num_workers: int
    _queue: asyncio.Queue[TQueueValue]
    _return_futures: bool
    _workers: Optional[List[asyncio.Future[None]]]

    def __init__(
        self,
        loop: Optional[asyncio.AbstractEventLoop],
        num_workers: int,
        name: str,
        logger: 'logging.Logger',
        worker_co: Callable[..., asyncio.Future[TWorkerResult]],
        load_factor: int = 1,
        job_accept_duration: Optional[datetime.timedelta] = None,
        max_task_time: Optional[int] = None,
        return_futures: bool = False,
        raise_on_join: Optional[bool] = False,
        log_every_n: Optional[int] = None,
        expected_total: Optional[int] = None
    ):
        """
        This class will create `num_workers` asyncio tasks to work against a queue of
        `num_workers * load_factor` items of back-pressure (IOW we will block after such
        number of items of work is in the queue).  `worker_co` will be called
        against each item retrieved from the queue. If any exceptions are raised out of
        worker_co, self.exceptions will be set to True.
        @param loop: asyncio loop to use
        @param num_workers: number of async tasks which will pull from the internal queue
        @param name: name of the worker pool (used for logging)
        @param logger: logger to use
        @param worker_co: async coroutine to call when an item is retrieved from the queue
        @param load_factor: multiplier used for number of items in queue
        @param job_accept_duration: maximum number of seconds from first push to last push before a TimeoutError will
                be thrown.
                Set to None for no limit.  Note this does not get reset on aenter/aexit.
        @param max_task_time: maximum time allowed for each task before a CancelledError is raised in the task.
                Set to None for no limit.
        @param return_futures: set to reture to return a future for each `push` (imposes CPU overhead)
        @param raise_on_join: raise on join if any exceptions have occurred, default is False
        @param log_every_n: (optional) set to number of `push`s each time a log statement should be printed (default
                does not print every-n pushes)
        @param expected_total: (optional) expected total number of jobs (used for `log_event_n` logging)
        @return: instance of AsyncWorkerPool
        """
        loop = loop or asyncio.get_event_loop()
        self._loop = loop
        self._num_workers = num_workers
        self._logger = logger
        self._queue = self.make_queue(num_workers, load_factor)
        self._workers = None
        self._exceptions = False
        self._job_accept_duration = job_accept_duration
        self._first_push_dt = None
        self._max_task_time = max_task_time
        self._return_futures = return_futures
        self._raise_on_join = raise_on_join
        self._name = name
        self._worker_co = worker_co
        self._total_queued = 0
        self._log_every_n = log_every_n
        self._expected_total = expected_total

    def make_queue(self, num_workers: int, load_factor: int) -> asyncio.Queue[TQueueValue]:
        raise NotImplementedError

    def put_future(self, future: Optional[asyncio.Future[TWorkerResult]], args: Any, kwargs: Any) -> Awaitable[None]:
        raise NotImplementedError

    def put_terminator(self) -> Awaitable[None]:
        raise NotImplementedError

    def get(self) -> Awaitable[TQueueItem]:
        return cast(Awaitable[TQueueItem], self._queue.get())

    async def _worker_loop(self) -> None:
        while True:
            got_obj = False
            future = None

            try:
                item = await self.get()
                got_obj = True

                if isinstance(item, Terminator):
                    break

                future, args, kwargs = item
                # the wait_for will cancel the task (task sees CancelledError) and raises a TimeoutError from here
                # so be wary of catching TimeoutErrors in this loop
                result = await asyncio.wait_for(self._worker_co(*args, **kwargs), self._max_task_time, loop=self._loop)

                if future:
                    future.set_result(result)
            except (KeyboardInterrupt, MemoryError, SystemExit) as e:
                if future:
                    future.set_exception(e)
                self._exceptions = True
                raise
            except BaseException as e:
                self._exceptions = True

                if future:
                    # don't log the failure when the client is receiving the future
                    future.set_exception(e)
                else:
                    self._logger.exception('Worker call failed')
            finally:
                if got_obj:
                    self._queue.task_done()

    @property
    def exceptions(self) -> bool:
        return self._exceptions

    @property
    def total_queued(self) -> int:
        return self._total_queued

    async def __aenter__(self) -> 'AsyncPoolBase':
        self.start()
        return self

    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[Exception],
        exc_tb: Optional[types.TracebackType]
    ) -> None:
        await self.join()

    async def push(self, *args: Any, **kwargs: Any) -> Optional[asyncio.Future[TWorkerResult]]:
        """ Method to push work to `worker_co` passed to `__init__`.
        :param args: position arguments to be passed to `worker_co`
        :param kwargs: keyword arguments to be passed to `worker_co`
        :return: future of result """
        if self._first_push_dt is None:
            self._first_push_dt = utc_now()

        if self._job_accept_duration is not None and (utc_now() - self._first_push_dt) > self._job_accept_duration:
            raise TimeoutError(
                "Maximum lifetime of {} seconds of AsyncWorkerPool: {} exceeded".format(
                    self._job_accept_duration, self._name
                )
            )

        generic_future = asyncio.futures.Future(loop=self._loop) if self._return_futures else None
        if TYPE_CHECKING:
            future = cast(Optional[asyncio.Future[TWorkerResult]], generic_future)
        else:
            future = generic_future
        await self.put_future(future, args, kwargs)

        self._total_queued += 1

        if self._log_every_n is not None and (self._total_queued % self._log_every_n) == 0:
            self._logger.info(
                "pushed {}/{} items to {} AsyncWorkerPool".format(self._total_queued, self._expected_total, self._name)
            )

        return future

    def start(self) -> None:
        """ Will start up worker pool and reset exception state """
        assert self._workers is None
        self._exceptions = False
        self._workers = [asyncio.ensure_future(self._worker_loop(), loop=self._loop) for _ in range(self._num_workers)]

    async def join(self) -> None:
        # no-op if workers aren't running
        if not self._workers:
            return

        self._logger.debug('Joining {}'.format(self._name))
        # The Terminators will kick each worker from being blocked against the _queue.get() and allow
        # each one to exit
        for _ in range(self._num_workers):
            await self.put_terminator()

        try:
            await asyncio.gather(*self._workers, loop=self._loop)
            self._workers = None
        except Exception:
            self._logger.exception('Exception joining {}'.format(self._name))
            raise
        finally:
            self._logger.debug('Completed {}'.format(self._name))

        if self._exceptions and self._raise_on_join:
            raise Exception("Exception occurred in pool {}".format(self._name))

    def is_empty(self) -> bool:
        return self._queue.empty()


class AsyncPool(AsyncPoolBase):

    def make_queue(self, num_workers: int, load_factor: int) -> asyncio.Queue[TQueueValue]:
        return asyncio.Queue(num_workers * load_factor)

    def put_future(self, future: Optional[asyncio.Future[TWorkerResult]], args: Any, kwargs: Any) -> Awaitable[None]:
        return self._queue.put((future, args, kwargs))

    def put_terminator(self) -> Awaitable[None]:
        return self._queue.put(Terminator())

    def get(self) -> Awaitable[TQueueItem]:
        return self._queue.get()


class PriorityAsyncPool(AsyncPoolBase, PriorityAsyncPoolInterface):
    # _queue: asyncio.PriorityQueue[TPrioirtyQueueValue]
    counter: int

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.counter = 0

    async def push(self, *args: Any, **kwargs: Any) -> Optional[asyncio.Future[TWorkerResult]]:
        return await super().push_with_priority(1000, *args, **kwargs)

    async def push_with_priority(self, priority: int, *args: Any,
                                 **kwargs: Any) -> Optional[asyncio.Future[TWorkerResult]]:
        return await super().push(priority, *args, **kwargs)

    def make_queue(self, num_workers: int, load_factor: int) -> asyncio.PriorityQueue[TPrioirtyQueueValue]:
        return asyncio.PriorityQueue(num_workers * load_factor)

    def put_future(self, future: Optional[asyncio.Future[TWorkerResult]], args: Any, kwargs: Any) -> Awaitable[None]:
        priority, args = args[0], args[1:]
        assert isinstance(priority, int) and priority < PRIORITY_TERMINATE
        # priority queueを使って例えば (priority, (future, func, args))みたいに値をいれると、priorityが同値の場合、次のfutureを
        # 比較につかってエラーがおこる
        # そのため、絶対同値にならない値をpriorityに混ぜておく必要がある
        self.counter += 1
        priority = (priority, self.counter)

        return self._queue.put((priority, (future, args, kwargs)))

    def put_terminator(self) -> Awaitable[None]:
        self.counter += 1
        return self._queue.put(((PRIORITY_TERMINATE, self.counter), Terminator()))

    async def get(self) -> TQueueItem:
        item = await self._queue.get()
        return cast(TQueueItem, cast(TPrioirtyQueueValue, item)[1])
