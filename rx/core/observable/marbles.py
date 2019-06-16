import threading
import re
from sys import maxsize
from typing import Any, List, Optional, Mapping, Tuple
from datetime import datetime, timedelta

from rx import Observable
from rx.core import notification, typing
from rx.disposable import CompositeDisposable, Disposable
from rx.scheduler import NewThreadScheduler
from rx.core.typing import RelativeTime, AbsoluteOrRelativeTime, Scheduler


new_thread_scheduler = NewThreadScheduler()

# tokens will be searched in the order below using pipe
# group of elements: match any characters surrounded by ()
pattern_group = r"(\(.*?\))"
# timespan: match one or multiple hyphens
pattern_ticks = r"(-+)"
# comma err: match any comma which is not in a group
pattern_comma_error = r"(,)"
# element: match | or # or one or more characters which are not - | # ( ) ,
pattern_element = r"(#|\||[^-,()#\|]+)"

pattern = r'|'.join([
    pattern_group,
    pattern_ticks,
    pattern_comma_error,
    pattern_element,
    ])
tokens = re.compile(pattern)


def hot(string: str,
        timespan: RelativeTime = 0.1,
        duetime: AbsoluteOrRelativeTime = 0.0,
        lookup: Optional[Mapping] = None,
        error: Optional[Exception] = None,
        scheduler: Optional[Scheduler] = None
        ) -> Observable:

    obs_scheduler = scheduler or new_thread_scheduler

    if isinstance(duetime, datetime):
        duetime = duetime - obs_scheduler.now

    messages = parse(
        string,
        timespan=timespan,
        time_shift=duetime,
        lookup=lookup,
        error=error,
        raise_stopped=True,
        )

    lock = threading.RLock()
    is_stopped = False
    gen_id = ~maxsize
    observers = {}

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        # should a hot observable already completed or on error
        # re-push on_completed/on_error at subscription time?
        nonlocal gen_id
        if not is_stopped:
            with lock:
                obs_id = gen_id
                gen_id += 1
                observers[obs_id] = (on_next, on_error, on_completed)

        def dispose():
            with lock:
                try:
                    del observers[obs_id]
                except ValueError:
                    pass

        return Disposable(dispose)

    def create_action(notification):
        def action(_: typing.Scheduler, __: Any = None) -> None:
            nonlocal is_stopped

            with lock:
                _observers = observers.copy().values()
            for on_next, on_error, on_completed in _observers:
                notification.accept(on_next, on_error, on_completed)

            if notification.kind in ('C', 'E'):
                is_stopped = True

        return action

    for message in messages:
        timespan, notification = message
        action = create_action(notification)

        # Don't make closures within a loop
        obs_scheduler.schedule_relative(timespan, action)

    return Observable(subscribe)


def from_marbles(string: str,
                 timespan: RelativeTime = 0.1,
                 lookup: Optional[Mapping] = None,
                 error: Optional[Exception] = None,
                 scheduler: Optional[Scheduler] = None
                 ) -> Observable:

    obs_scheduler = scheduler
    messages = parse(string, timespan=timespan, lookup=lookup, error=error, raise_stopped=True)

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        sub_scheduler = obs_scheduler or scheduler or new_thread_scheduler
        disp = CompositeDisposable()

        def schedule_msg(message):
            duetime, notification = message

            def action(_: typing.Scheduler, __: Any = None) -> None:
                notification.accept(on_next, on_error, on_completed)

            disp.add(sub_scheduler.schedule_relative(duetime, action))

        for message in messages:
            # Don't make closures within a loop
            schedule_msg(message)

        return disp
    return Observable(subscribe)


def parse(string: str,
          timespan: RelativeTime = 1.0,
          time_shift: RelativeTime = 0.0,
          lookup: Optional[Mapping] = None,
          error: Optional[Exception] = None,
          raise_stopped: bool = False
          ) -> List[Tuple[RelativeTime, notification.Notification]]:
    """Convert a marble diagram string to a list of messages.

    Each character in the string will advance time by timespan
    (exept for space). Characters that are not special (see the table below)
    will be interpreted as a value to be emitted. numbers will be cast
    to int or float.

    Special characters:
        +--------+--------------------------------------------------------+
        |  `-`   | advance time by timespan                               |
        +--------+--------------------------------------------------------+
        |  `#`   | on_error()                                             |
        +--------+--------------------------------------------------------+
        |  `|`   | on_completed()                                         |
        +--------+--------------------------------------------------------+
        |  `(`   | open a group of elements sharing the same timestamp    |
        +--------+--------------------------------------------------------+
        |  `)`   | close a group of elements                              |
        +--------+--------------------------------------------------------+
        |  `,`   | separate elements in a group                           |
        +--------+--------------------------------------------------------+
        | space  | used to align multiple diagrams, does not advance time |
        +--------+--------------------------------------------------------+

    In a group of elements, the position of the initial `(` determines the
    timestamp at which grouped elements will be emitted. E.g. `--(12,3,4)--`
    will emit 12, 3, 4 at 2 * timespan and then advance virtual time
    by 8 * timespan.

    Examples:
        >>> parse("--1--(2,3)-4--|")
        >>> parse("a--b--c-", lookup={'a': 1, 'b': 2, 'c': 3})
        >>> parse("a--b---#", error=ValueError("foo"))

    Args:
        string: String with marble diagram

        timespan: [Optional] duration of each character in second.
            If not specified, defaults to 0.1s.

        lookup: [Optional] dict used to convert an element into a specified
            value. If not specified, defaults to {}.

        time_shift: [Optional] time used to delay every elements.
            If not specified, defaults to 0.0s.

        error: [Optional] exception that will be use in place of the # symbol.
            If not specified, defaults to Exception('error').

        raise_finished: [optional] raise ValueError if elements are
            declared after on_completed or on_error symbol.

    Returns:
        A list of messages defined as a tuple of (timespan, notification).

    """

    error = error or Exception('error')
    lookup = lookup or {}

    if isinstance(timespan, timedelta):
        timespan = timespan.total_seconds()
    if isinstance(time_shift, timedelta):
        time_shift = time_shift.total_seconds()

    string = string.replace(' ', '')

    # try to cast a string to an int, then to a float
    def try_number(element):
        try:
            return int(element)
        except ValueError:
            try:
                return float(element)
            except ValueError:
                return element

    def map_element(time, element):
        if element == '|':
            return (time, notification.OnCompleted())
        elif element == '#':
            return (time, notification.OnError(error))
        else:
            value = try_number(element)
            value = lookup.get(value, value)
            return (time, notification.OnNext(value))

    is_stopped = False

    def check_stopped(element):
        nonlocal is_stopped
        if raise_stopped:
            if is_stopped:
                raise ValueError('Elements cannot be declared after a # or | symbol.')

            if element in ('#', '|'):
                is_stopped = True

    iframe = 0
    messages = []

    for results in tokens.findall(string):
        timestamp = iframe * timespan + time_shift
        group, ticks, comma_error, element = results

        if group:
            elements = group[1:-1].split(',')
            for elm in elements:
                check_stopped(elm)
            grp_messages = [map_element(timestamp, elm) for elm in elements if elm !='']
            messages.extend(grp_messages)
            iframe += len(group)

        if ticks:
            iframe += len(ticks)

        if comma_error:
            raise ValueError("Comma is only allowed in group of elements.")

        if element:
            check_stopped(element)
            message = map_element(timestamp, element)
            messages.append(message)
            iframe += len(element)

    return messages
