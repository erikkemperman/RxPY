from asyncio.futures import Future
from typing import Optional

from rx.disposable import Disposable
from rx.core import typing
from rx.core import Observable


def _from_future(future: Future) -> Observable:
    """Converts a Future to an Observable sequence

    Args:
        future -- A Python 3 compatible future.
            https://docs.python.org/3/library/asyncio-task.html#future
            http://www.tornadoweb.org/en/stable/concurrent.html#tornado.concurrent.Future

    Returns:
        An Observable sequence which wraps the existing future success
        and failure.
    """

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:

        def done(future):
            try:
                value = future.result()
            except Exception as ex:  # pylint: disable=broad-except
                if on_error is not None:
                    on_error(ex)
            else:
                if on_next is not None:
                    on_next(value)
                if on_completed is not None:
                    on_completed()

        future.add_done_callback(done)

        def dispose() -> None:
            if future and future.cancel:
                future.cancel()

        return Disposable(dispose)

    return Observable(subscribe)
