from typing import Iterable, Optional

from rx.disposable import Disposable
from rx.core import Observable, typing
from rx.disposable import SingleAssignmentDisposable, CompositeDisposable, SerialDisposable
from rx.scheduler import current_thread_scheduler


def _concat_with_iterable(sources: Iterable[Observable]) -> Observable:

    sources_ = iter(sources)

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        _scheduler = scheduler or current_thread_scheduler

        subscription = SerialDisposable()
        cancelable = SerialDisposable()
        is_disposed = False

        def action(action1, state=None):
            nonlocal is_disposed
            if is_disposed:
                return

            def _on_completed():
                cancelable.disposable = _scheduler.schedule(action)

            try:
                current = next(sources_)
            except StopIteration:
                if on_completed is not None:
                    on_completed()
            except Exception as ex:  # pylint: disable=broad-except
                if on_error is not None:
                    on_error(ex)
            else:
                d = SingleAssignmentDisposable()
                subscription.disposable = d
                d.disposable = current.subscribe(
                    on_next,
                    on_error,
                    _on_completed,
                    scheduler=_scheduler
                )

        cancelable.disposable = _scheduler.schedule(action)

        def dispose():
            nonlocal is_disposed
            is_disposed = True

        return CompositeDisposable(subscription, cancelable, Disposable(dispose))
    return Observable(subscribe)
