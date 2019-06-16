from typing import Callable, Optional

from rx.core import Observable, typing
from rx.core.typing import Disposable
from rx.disposable import CompositeDisposable


def _do_action(_next: Optional[typing.OnNext] = None,
               _error: Optional[typing.OnError] = None,
               _completed: Optional[typing.OnCompleted] = None
               ) -> Callable[[Observable], Observable]:
    def do_action(source: Observable) -> Observable:
        """Invokes an action for each element in the observable
        sequence and invokes an action on graceful or exceptional
        termination of the observable sequence. This method can be used
        for debugging, logging, etc. of query behavior by intercepting
        the message stream to run arbitrary actions for messages on the
        pipeline.

        Examples:
            >>> do_action(send)(observable)
            >>> do_action(on_next, on_error)(observable)
            >>> do_action(on_next, on_error, on_completed)(observable)

        Args:
            on_next: [Optional] Action to invoke for each element in
                the observable sequence.
            on_error: [Optional] Action to invoke on exceptional
                termination of the observable sequence.
            on_completed: [Optional] Action to invoke on graceful
                termination of the observable sequence.

        Returns:
            An observable source sequence with the side-effecting
            behavior applied.
        """

        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:
            def _on_next(x):
                if _next is None:
                    if on_next is not None:
                        on_next(x)
                else:
                    try:
                        _next(x)
                    except Exception as e:  # pylint: disable=broad-except
                        if on_error is not None:
                            on_error(e)

                    if on_next is not None:
                        on_next(x)

            def _on_error(exception):
                if _error is None:
                    if on_error is not None:
                        on_error(exception)
                else:
                    try:
                        _error(exception)
                    except Exception as e:  # pylint: disable=broad-except
                        if on_error is not None:
                            on_error(e)
                    else:
                        if on_error is not None:
                            on_error(exception)

            def _on_completed():
                if _completed is None:
                    if on_completed is not None:
                        on_completed()
                else:
                    try:
                        _completed()
                    except Exception as e:   # pylint: disable=broad-except
                        if on_error is not None:
                            on_error(e)

                    if on_completed is not None:
                        on_completed()

            return source.subscribe(
                _on_next,
                _on_error,
                _on_completed,
                scheduler=scheduler
            )
        return Observable(subscribe)
    return do_action


def do(on_next: Optional[typing.OnNext] = None,
       on_error: Optional[typing.OnError] = None,
       on_completed: Optional[typing.OnCompleted] = None
       ) -> Callable[[Observable], Observable]:
    """Invokes an action for each element in the observable sequence and
    invokes an action on graceful or exceptional termination of the
    observable sequence. This method can be used for debugging, logging,
    etc. of query behavior by intercepting the message stream to run
    arbitrary actions for messages on the pipeline.

    >>> do(on_next, on_error, on_completed)

    Args:
        on_next: [Optional] Action to invoke for each element in
                the observable sequence.
        on_error: [Optional] Action to invoke on exceptional
            termination of the observable sequence.
        on_completed: [Optional] Action to invoke on graceful
            termination of the observable sequence.

    Returns:
        An operator function that takes the source observable and
        returns the source sequence with the side-effecting behavior
        applied.
    """

    return _do_action(on_next, on_error, on_completed)


def do_after_next(source, after_next):
    """Invokes an action with each element after it has been emitted downstream.
    This can be helpful for debugging, logging, and other side effects.

    after_next -- Action to invoke on each element after it has been emitted
    """

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:

        def _on_next(value):
            try:
                if on_next is not None:
                    on_next(value)
                after_next(value)
            except Exception as e:  # pylint: disable=broad-except
                if on_error is not None:
                    on_error(e)

        return source.subscribe(
            _on_next,
            on_error,
            on_completed
        )
    return Observable(subscribe)


def do_on_subscribe(source: Observable, on_subscribe):
    """Invokes an action on subscription.

    This can be helpful for debugging, logging, and other side effects
    on the start of an operation.

    Args:
        on_subscribe: Action to invoke on subscription
    """

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        on_subscribe()
        return source.subscribe(
            on_next,
            on_error,
            on_completed,
            scheduler=scheduler
        )

    return Observable(subscribe)


def do_on_dispose(source: Observable, on_dispose):
    """Invokes an action on disposal.

     This can be helpful for debugging, logging, and other side effects
     on the disposal of an operation.

    Args:
        on_dispose: Action to invoke on disposal
    """

    class OnDispose(Disposable):

        def dispose(self) -> None:
            on_dispose()

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        composite_disposable = CompositeDisposable()
        composite_disposable.add(OnDispose())
        subscription = source.subscribe(
            on_next,
            on_error,
            on_completed,
            scheduler=scheduler
        )
        composite_disposable.add(subscription)
        return composite_disposable

    return Observable(subscribe)


def do_on_terminate(source, on_terminate):
    """Invokes an action on an on_complete() or on_error() event.
     This can be helpful for debugging, logging, and other side effects
     when completion or an error terminates an operation.


    on_terminate -- Action to invoke when on_complete or throw is called
    """

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:

        def _on_completed():
            try:
                on_terminate()
            except Exception as err:  # pylint: disable=broad-except
                if on_error is not None:
                    on_error(err)
            else:
                if on_completed is not None:
                    on_completed()

        def _on_error(exception):
            try:
                on_terminate()
            except Exception as err:  # pylint: disable=broad-except
                if on_error is not None:
                    on_error(err)
            else:
                if on_error is not None:
                    on_error(exception)

        return source.subscribe(
            on_next,
            _on_error,
            _on_completed,
            scheduler=scheduler
        )
    return Observable(subscribe)


def do_after_terminate(source, after_terminate):
    """Invokes an action after an on_complete() or on_error() event.
     This can be helpful for debugging, logging, and other side effects
     when completion or an error terminates an operation


    on_terminate -- Action to invoke after on_complete or throw is called
    """

    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:

        def _on_completed():
            if on_completed is not None:
                on_completed()
            try:
                after_terminate()
            except Exception as err:  # pylint: disable=broad-except
                if on_error is not None:
                    on_error(err)

        def _on_error(exception):
            if on_error is not None:
                on_error(exception)
            try:
                after_terminate()
            except Exception as err:  # pylint: disable=broad-except
                if on_error is not None:
                    on_error(err)

        return source.subscribe(
            on_next,
            _on_error,
            _on_completed,
            scheduler=scheduler
        )
    return Observable(subscribe)


def do_finally(finally_action: Callable) -> Callable[[Observable], Observable]:
    """Invokes an action after an on_complete(), on_error(), or disposal
    event occurs.

    This can be helpful for debugging, logging, and other side effects
    when completion, an error, or disposal terminates an operation.

    Note this operator will strive to execute the finally_action once,
    and prevent any redudant calls

    Args:
        finally_action -- Action to invoke after on_complete, on_error,
        or disposal is called
    """

    class OnDispose(Disposable):
        def __init__(self, was_invoked):
            self.was_invoked = was_invoked

        def dispose(self) -> None:
            if not self.was_invoked[0]:
                finally_action()
                self.was_invoked[0] = True

    def partial(source: Observable) -> Observable:
        def subscribe(on_next: Optional[typing.OnNext] = None,
                      on_error: Optional[typing.OnError] = None,
                      on_completed: Optional[typing.OnCompleted] = None,
                      scheduler: Optional[typing.Scheduler] = None
                      ) -> typing.Disposable:

            was_invoked = [False]

            def _on_completed():
                if on_completed is not None:
                    on_completed()
                try:
                    if not was_invoked[0]:
                        finally_action()
                        was_invoked[0] = True
                except Exception as err:  # pylint: disable=broad-except
                    if on_error is not None:
                        on_error(err)

            def _on_error(exception):
                if on_error is not None:
                    on_error(exception)
                try:
                    if not was_invoked[0]:
                        finally_action()
                        was_invoked[0] = True
                except Exception as err:  # pylint: disable=broad-except
                    if on_error is not None:
                        on_error(err)

            composite_disposable = CompositeDisposable()
            composite_disposable.add(OnDispose(was_invoked))
            subscription = source.subscribe(
                on_next,
                _on_error,
                _on_completed,
                scheduler=scheduler
            )
            composite_disposable.add(subscription)

            return composite_disposable

        return Observable(subscribe)
    return partial
