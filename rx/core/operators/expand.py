from collections import deque
from typing import Callable, Optional

from rx.core import Observable, typing
from rx.core.typing import Mapper
from rx.disposable import SerialDisposable, CompositeDisposable, SingleAssignmentDisposable
from rx.scheduler import immediate_scheduler


def _expand(mapper: Mapper) -> Callable[[Observable], Observable]:
    def expand(source: Observable) -> Observable:
        """Expands an observable sequence by recursively invoking
        mapper.

        Args:
            source: Source obserable to expand.

        Returns:
            An observable sequence containing all the elements produced
            by the recursive expansion.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            _scheduler = scheduler or immediate_scheduler

            queue = deque()
            m = SerialDisposable()
            d = CompositeDisposable(m)
            active_count = [0]
            is_acquired = [False]

            def ensure_active():
                is_owner = False
                if queue:
                    is_owner = not is_acquired[0]
                    is_acquired[0] = True

                def action(scheduler, state):
                    if queue:
                        work = queue.popleft()
                    else:
                        is_acquired[0] = False
                        return

                    sad = SingleAssignmentDisposable()
                    d.add(sad)

                    def _on_next(value):
                        if on_next is not None:
                            on_next(value)
                        result = None
                        try:
                            result = mapper(value)
                        except Exception as ex:
                            if on_error is not None:
                                on_error(ex)
                            return

                        queue.append(result)
                        active_count[0] += 1
                        ensure_active()

                    def _on_complete():
                        d.remove(sad)
                        active_count[0] -= 1
                        if active_count[0] == 0 and on_completed is not None:
                            on_completed()

                    sad.disposable = work.subscribe(
                        _on_next,
                        on_error,
                        _on_complete,
                        scheduler=_scheduler
                    )
                    m.disposable = _scheduler.schedule(action)

                if is_owner:
                    m.disposable = _scheduler.schedule(action)

            queue.append(source)
            active_count[0] += 1
            ensure_active()
            return d
        return Observable(subscribe)
    return expand
