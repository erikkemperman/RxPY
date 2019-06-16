from datetime import datetime
from typing import Any, Optional

from rx.scheduler import timeout_scheduler
from rx.core import Observable, typing
from rx.disposable import MultipleAssignmentDisposable


def observable_timer_date(duetime, scheduler: Optional[typing.Scheduler] = None):

    obs_scheduler = scheduler

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        sub_scheduler = obs_scheduler or scheduler or timeout_scheduler

        def action(_: typing.Scheduler, __: Any = None) -> None:
            if on_next is not None:
                on_next(0)
            if on_completed is not None:
                on_completed()

        return sub_scheduler.schedule_absolute(duetime, action)
    return Observable(subscribe)


def observable_timer_duetime_and_period(duetime,
                                        period,
                                        scheduler: Optional[typing.Scheduler] = None
                                        ) -> Observable:

    obs_scheduler = scheduler

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        sub_scheduler = obs_scheduler or scheduler or timeout_scheduler
        nonlocal duetime

        if not isinstance(duetime, datetime):
            duetime = sub_scheduler.now + sub_scheduler.to_timedelta(duetime)

        p = max(0.0, sub_scheduler.to_seconds(period))
        mad = MultipleAssignmentDisposable()
        dt = [duetime]
        count = [0]

        def action(act_scheduler: typing.Scheduler, _: Any = None) -> None:
            if p > 0.0:
                now = act_scheduler.now
                dt[0] = dt[0] + act_scheduler.to_timedelta(p)
                if dt[0] <= now:
                    dt[0] = now + act_scheduler.to_timedelta(p)

            if on_next is not None:
                on_next(count[0])
            count[0] += 1
            mad.disposable = act_scheduler.schedule_absolute(dt[0], action)

        mad.disposable = sub_scheduler.schedule_absolute(dt[0], action)
        return mad
    return Observable(subscribe)


def observable_timer_timespan(duetime: typing.RelativeTime,
                              scheduler: Optional[typing.Scheduler] = None
                              ) -> Observable:
    obs_scheduler = scheduler

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        sub_scheduler = obs_scheduler or scheduler or timeout_scheduler

        d = sub_scheduler.to_seconds(duetime)

        def action(_: typing.Scheduler, __: Any = None) -> None:
            if on_next is not None:
                on_next(0)
            if on_completed is not None:
                on_completed()

        if d <= 0.0:
            return sub_scheduler.schedule(action)
        return sub_scheduler.schedule_relative(d, action)
    return Observable(subscribe)


def observable_timer_timespan_and_period(duetime: typing.RelativeTime,
                                         period: typing.RelativeTime,
                                         scheduler: Optional[typing.Scheduler] = None
                                         ) -> Observable:
    obs_scheduler = scheduler

    if duetime == period:
        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            sub_scheduler = obs_scheduler or scheduler or timeout_scheduler

            def action(count: int) -> int:
                if on_next is not None:
                    on_next(count)
                return count + 1

            return sub_scheduler.schedule_periodic(period, action, state=0)
        return Observable(subscribe)
    return observable_timer_duetime_and_period(duetime, period, scheduler)


def _timer(duetime: typing.AbsoluteOrRelativeTime,
           period: Optional[typing.RelativeTime] = None,
           scheduler: Optional[typing.Scheduler] = None
           ) -> Observable:
    if isinstance(duetime, datetime):
        if period is None:
            return observable_timer_date(duetime, scheduler)
        else:
            return observable_timer_duetime_and_period(duetime, period, scheduler)

    if period is None:
        return observable_timer_timespan(duetime, scheduler)

    return observable_timer_timespan_and_period(duetime, period, scheduler)
