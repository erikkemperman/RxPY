from datetime import datetime
from typing import Optional

from rx.scheduler import timeout_scheduler
from rx.core import Observable, typing
from rx.disposable import MultipleAssignmentDisposable


def observable_timer_date(duetime, scheduler: Optional[typing.Scheduler] = None):
    def subscribe(observer, scheduler_=None):
        _scheduler = scheduler or scheduler_

        def action(scheduler, state):
            observer.on_next(0)
            observer.on_completed()

        return _scheduler.schedule_absolute(duetime, action)
    return Observable(subscribe)


def observable_timer_duetime_and_period(duetime, period, scheduler: Optional[typing.Scheduler] = None) -> Observable:
    def subscribe(observer, scheduler_=None):
        _scheduler = scheduler or scheduler_ or timeout_scheduler
        nonlocal duetime

        if not isinstance(duetime, datetime):
            duetime = _scheduler.now + _scheduler.to_timedelta(duetime)

        p = max(0.0, _scheduler.to_seconds(period))
        mad = MultipleAssignmentDisposable()
        dt = [duetime]
        count = [0]

        def action(scheduler, state):
            if p > 0.0:
                now = scheduler.now
                dt[0] = dt[0] + scheduler.to_timedelta(p)
                if dt[0] <= now:
                    dt[0] = now + scheduler.to_timedelta(p)

            observer.on_next(count[0])
            count[0] += 1
            mad.disposable = scheduler.schedule_absolute(dt[0], action)
        mad.disposable = _scheduler.schedule_absolute(dt[0], action)
        return mad
    return Observable(subscribe)


def observable_timer_timespan(duetime: typing.RelativeTime, scheduler: Optional[typing.Scheduler] = None) -> Observable:
    def subscribe(observer, scheduler_=None):
        _scheduler = scheduler or scheduler_ or timeout_scheduler
        d = _scheduler.to_seconds(duetime)

        def action(scheduler, state):
            observer.on_next(0)
            observer.on_completed()

        if d <= 0.0:
            return _scheduler.schedule(action)
        return _scheduler.schedule_relative(d, action)
    return Observable(subscribe)


def observable_timer_timespan_and_period(duetime: typing.RelativeTime,
                                         period: typing.RelativeTime,
                                         scheduler: Optional[typing.Scheduler] = None
                                         ) -> Observable:
    if duetime == period:
        def subscribe(observer, scheduler_=None):
            _scheduler = scheduler or scheduler_ or timeout_scheduler

            def action(count):
                observer.on_next(count)
                return count + 1

            return _scheduler.schedule_periodic(period, action, state=0)
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
