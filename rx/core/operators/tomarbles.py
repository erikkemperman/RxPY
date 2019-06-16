from typing import List, Optional

from rx.core import Observable, typing
from rx.core.typing import Scheduler, RelativeTime
from rx.scheduler import NewThreadScheduler

new_thread_scheduler = NewThreadScheduler()


def _to_marbles(scheduler: Optional[Scheduler] = None, timespan: RelativeTime = 0.1):

    def to_marbles(source: Observable) -> Observable:
        """Convert an observable sequence into a marble diagram string.

        Args:
            timespan: [Optional] duration of each character in second.
                If not specified, defaults to 0.1s.
            scheduler: [Optional] The scheduler used to run the the input
                sequence on.

        Returns:
            Observable stream.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            _scheduler = scheduler or new_thread_scheduler

            result: List[str] = []
            last = _scheduler.now

            def add_timespan():
                nonlocal last

                now = _scheduler.now
                diff = now - last
                last = now
                secs = _scheduler.to_seconds(diff)
                dashes = "-" * int((secs + timespan / 2.0) * (1.0 / timespan))
                result.append(dashes)

            def _on_next(value):
                add_timespan()
                result.append(stringify(value))

            def _on_error(exception):
                add_timespan()
                result.append(stringify(exception))
                if on_next is not None:
                    on_next("".join(n for n in result))
                if on_completed is not None:
                    on_completed()

            def _on_completed():
                add_timespan()
                result.append("|")
                if on_next is not None:
                    on_next("".join(n for n in result))
                if on_completed is not None:
                    on_completed()

            return source.subscribe(
                _on_next,
                _on_error,
                _on_completed
            )
        return Observable(subscribe)
    return to_marbles


def stringify(value):
    """Utility for stringifying an event.
    """
    string = str(value)
    if len(string) > 1:
        string = "(%s)" % string

    return string
