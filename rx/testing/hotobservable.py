from sys import maxsize
from threading import RLock
from typing import Dict, List, Optional, Tuple

from rx.disposable import Disposable
from rx.core import Observable, typing
from rx.scheduler import VirtualTimeScheduler

from .recorded import Recorded
from .subscription import Subscription


class HotObservable(Observable):
    def __init__(self, scheduler: VirtualTimeScheduler, messages: List[Recorded]) -> None:
        super().__init__()

        self.scheduler: VirtualTimeScheduler = scheduler
        self.messages = messages
        self.subscriptions: List[Subscription] = []
        self.observers: Dict[int, Tuple[
            Optional[typing.OnNext],
            Optional[typing.OnError],
            Optional[typing.OnCompleted]
        ]] = {}
        self.lock = RLock()
        self.gen_id = ~maxsize

        def get_action(notification):
            def action(scheduler, state):
                with self.lock:
                    observers = self.observers.copy().values()
                for on_next, on_error, on_completed in observers:
                    notification.accept(on_next, on_error, on_completed)
                return Disposable()
            return action

        for message in self.messages:
            notification = message.value

            # Warning: Don't make closures within a loop
            action = get_action(notification)
            scheduler.schedule_absolute(message.time, action)

    def _subscribe_core(self,
                        on_next: Optional[typing.OnNext] = None,
                        on_error: Optional[typing.OnError] = None,
                        on_completed: Optional[typing.OnCompleted] = None,
                        scheduler: Optional[typing.Scheduler] = None
                        ) -> typing.Disposable:
        with self.lock:
            obs_id = self.gen_id
            self.gen_id += 1
        self.observers[obs_id] = on_next, on_error, on_completed
        self.subscriptions.append(Subscription(self.scheduler.clock))
        index = len(self.subscriptions) - 1

        def dispose_action():
            del self.observers[obs_id]
            start = self.subscriptions[index].subscribe
            end = self.scheduler.clock
            self.subscriptions[index] = Subscription(start, end)

        return Disposable(dispose_action)
