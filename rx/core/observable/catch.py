from typing import Iterable, Optional

from rx.disposable import Disposable
from rx.core import Observable, typing
from rx.disposable import SingleAssignmentDisposable, CompositeDisposable, SerialDisposable
from rx.scheduler import current_thread_scheduler


def _catch_with_iterable(sources: Iterable[Observable]) -> Observable:

    """Continues an observable sequence that is terminated by an
    exception with the next observable sequence.

    Examples:
        >>> res = catch([xs, ys, zs])
        >>> res = rx.catch(src for src in [xs, ys, zs])

    Args:
        sources: an Iterable of observables. Thus a generator is accepted.

    Returns:
        An observable sequence containing elements from consecutive
        source sequences until a source sequence terminates
        successfully.
    """

    sources_ = iter(sources)

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        _scheduler = scheduler or current_thread_scheduler

        subscription = SerialDisposable()
        cancelable = SerialDisposable()
        last_exception = [None]
        is_disposed = []

        def action(action1, state=None):
            def _on_error(exn):
                last_exception[0] = exn
                cancelable.disposable = _scheduler.schedule(action)

            if is_disposed:
                return

            try:
                current = next(sources_)
            except StopIteration:
                if last_exception[0]:
                    if on_error is not None:
                        on_error(last_exception[0])
                elif on_completed is not None:
                    on_completed()
            except Exception as ex:  # pylint: disable=broad-except
                if on_error is not None:
                    on_error(ex)
            else:
                d = SingleAssignmentDisposable()
                subscription.disposable = d
                d.disposable = current.subscribe(
                    on_next,
                    _on_error,
                    on_completed,
                    scheduler=_scheduler
                )

        cancelable.disposable = _scheduler.schedule(action)

        def dispose():
            is_disposed.append(True)
        return CompositeDisposable(subscription, cancelable, Disposable(dispose))
    return Observable(subscribe)
