from typing import Callable, Optional, Union

import rx
from rx.core import Observable, typing
from rx.disposable import CompositeDisposable


def sample_observable(source: Observable, sampler: Observable) -> Observable:
    def subscribe(on_next: Optional[typing.OnNext] = None,
                  on_error: Optional[typing.OnError] = None,
                  on_completed: Optional[typing.OnCompleted] = None,
                  scheduler: Optional[typing.Scheduler] = None
                  ) -> typing.Disposable:
        at_end = [None]
        has_value = [None]
        value = [None]

        def sample_subscribe(x=None):
            if has_value[0]:
                has_value[0] = False
                if on_next is not None:
                    on_next(value[0])

            if at_end[0] and on_completed is not None:
                on_completed()

        def _on_next(new_value):
            has_value[0] = True
            value[0] = new_value

        def _on_completed():
            at_end[0] = True

        return CompositeDisposable(
            source.subscribe(
                _on_next,
                on_error,
                _on_completed,
                scheduler=scheduler
            ),
            sampler.subscribe(
                sample_subscribe,
                on_error,
                sample_subscribe,
                scheduler=scheduler
            )
        )
    return Observable(subscribe)


def _sample(sampler: Union[typing.RelativeTime, Observable],
            scheduler: Optional[typing.Scheduler] = None
            ) -> Callable[[Observable], Observable]:

    def sample(source: Observable) -> Observable:
        """Samples the observable sequence at each interval.

        Examples:
            >>> res = sample(source)

        Args:
            source: Source sequence to sample.

        Returns:
            Sampled observable sequence.
        """

        if isinstance(sampler, typing.Observable):
            return sample_observable(source, sampler)
        else:
            return sample_observable(source, rx.interval(sampler, scheduler=scheduler))

    return sample
