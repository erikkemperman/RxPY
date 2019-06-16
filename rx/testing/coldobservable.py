from typing import List, Optional

from rx.disposable import Disposable, CompositeDisposable
from rx.core import Observable, typing
from rx.scheduler import VirtualTimeScheduler

from .subscription import Subscription


class ColdObservable(Observable):
    def __init__(self, scheduler: VirtualTimeScheduler, messages) -> None:
        super().__init__()

        self.scheduler: VirtualTimeScheduler = scheduler
        self.messages = messages
        self.subscriptions: List[Subscription] = []

    def _subscribe_core(self,
                        on_next: Optional[typing.OnNext] = None,
                        on_error: Optional[typing.OnError] = None,
                        on_completed: Optional[typing.OnCompleted] = None,
                        scheduler: Optional[typing.Scheduler] = None
                        ) -> typing.Disposable:
        self.subscriptions.append(Subscription(self.scheduler.clock))
        index = len(self.subscriptions) - 1
        disp = CompositeDisposable()

        def get_action(notification):
            def action(scheduler, state):
                notification.accept(on_next, on_error, on_completed)
                return Disposable()
            return action

        for message in self.messages:
            notification = message.value

            # Don't make closures within a loop
            action = get_action(notification)
            disp.add(self.scheduler.schedule_relative(message.time, action))

        def dispose() -> None:
            start = self.subscriptions[index].subscribe
            end = self.scheduler.to_seconds(self.scheduler.now)
            self.subscriptions[index] = Subscription(start, end)
            disp.dispose()

        return Disposable(dispose)
