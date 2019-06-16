import logging
from typing import Any, Callable, Optional

from rx import empty
from rx.core import Observable, typing
from rx.internal import noop
from rx.internal.utils import add_ref
from rx.disposable import SingleAssignmentDisposable, SerialDisposable, CompositeDisposable, RefCountDisposable
from rx.subject import Subject
from rx import operators as ops

log = logging.getLogger("Rx")


def _window_toggle(openings: Observable,
                   closing_mapper: Callable[[Any], Observable]
                   ) -> Callable[[Observable], Observable]:
    """Projects each element of an observable sequence into zero or
    more windows.

    Args:
        source: Source observable to project into windows.

    Returns:
        An observable sequence of windows.
    """
    def window_toggle(source: Observable) -> Observable:

        def mapper(args):
            _, window = args
            return window

        return openings.pipe(
            ops.group_join(
                source,
                closing_mapper,
                lambda _: empty(),
                ),
            ops.map(mapper),
            )
    return window_toggle


def _window(boundaries: Observable) -> Callable[[Observable], Observable]:
    """Projects each element of an observable sequence into zero or
    more windows.

    Args:
        source: Source observable to project into windows.

    Returns:
        An observable sequence of windows.
    """
    def window(source: Observable) -> Observable:
        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            window_subject = Subject()
            d = CompositeDisposable()
            r = RefCountDisposable(d)

            ref = add_ref(window_subject, r)
            if on_next is not None:
                on_next(ref)

            def _on_next_window(x):
                window_subject.on_next(x)

            def _on_error(err):
                window_subject.on_error(err)
                if on_error is not None:
                    on_error(err)

            def _on_completed():
                window_subject.on_completed()
                if on_completed is not None:
                    on_completed()

            d.add(source.subscribe(
                _on_next_window,
                _on_error,
                _on_completed,
                scheduler=scheduler
            ))

            def _on_next_observer(w):
                nonlocal window_subject
                window_subject.on_completed()
                window_subject = Subject()
                ref = add_ref(window_subject, r)
                if on_next is not None:
                    on_next(ref)

            d.add(boundaries.subscribe(
                _on_next_observer,
                _on_error,
                _on_completed,
                scheduler=scheduler
            ))
            return r
        return Observable(subscribe)
    return window


def _window_when(closing_mapper: Callable[[], Observable]) -> Callable[[Observable], Observable]:
    """Projects each element of an observable sequence into zero or
    more windows.

    Args:
        source: Source observable to project into windows.

    Returns:
        An observable sequence of windows.
    """
    def window_when(source: Observable) -> Observable:

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            m = SerialDisposable()
            d = CompositeDisposable(m)
            r = RefCountDisposable(d)
            window = Subject()

            ref = add_ref(window, r)
            if on_next is not None:
                on_next(ref)

            def _on_next(value):
                window.on_next(value)

            def _on_error(error):
                window.on_error(error)
                if on_error is not None:
                    on_error(error)

            def _on_completed():
                window.on_completed()
                if on_completed is not None:
                    on_completed()

            d.add(source.subscribe(
                _on_next,
                _on_error,
                _on_completed,
                scheduler=scheduler
            ))

            def create_window_on_completed():
                try:
                    window_close = closing_mapper()
                except Exception as exception:
                    if on_error is not None:
                        on_error(exception)
                    return

                def _on_completed():
                    nonlocal window
                    window.on_completed()
                    window = Subject()
                    ref = add_ref(window, r)
                    if on_next is not None:
                        on_next(ref)
                    create_window_on_completed()

                m1 = SingleAssignmentDisposable()
                m.disposable = m1
                m1.disposable = window_close.pipe(ops.take(1)).subscribe(
                    noop,
                    _on_error,
                    _on_completed,
                    scheduler=scheduler
                )

            create_window_on_completed()
            return r
        return Observable(subscribe)
    return window_when
