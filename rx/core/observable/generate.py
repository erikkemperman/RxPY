from typing import Any, Optional

from rx.core import Observable, typing
from rx.core.typing import Mapper, Predicate
from rx.scheduler import current_thread_scheduler
from rx.disposable import MultipleAssignmentDisposable


def _generate(initial_state: Any,
              condition: Predicate,
              iterate: Mapper
              ) -> Observable:
    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        _scheduler = scheduler or current_thread_scheduler
        first = True
        state = initial_state
        mad = MultipleAssignmentDisposable()

        def action(scheduler, state1=None):
            nonlocal first
            nonlocal state

            has_result = False
            result = None

            try:
                if first:
                    first = False
                else:
                    state = iterate(state)

                has_result = condition(state)
                if has_result:
                    result = state

            except Exception as exception:  # pylint: disable=broad-except
                if on_error is not None:
                    on_error(exception)
                return

            if has_result:
                if on_next is not None:
                    on_next(result)
                mad.disposable = scheduler.schedule(action)
            elif on_completed is not None:
                on_completed()

        mad.disposable = _scheduler.schedule(action)
        return mad
    return Observable(subscribe)
