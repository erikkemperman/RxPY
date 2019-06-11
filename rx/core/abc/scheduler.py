from abc import ABC, abstractmethod


class Scheduler(ABC):
    """Scheduler abstract base class. Untyped."""

    @property
    @abstractmethod
    def now(self):
        return NotImplemented

    @abstractmethod
    def schedule(self, action, state=None):
        return NotImplemented

    @abstractmethod
    def schedule_relative(self, relative, action, state=None):
        return NotImplemented

    @abstractmethod
    def schedule_absolute(self, absolute, action, state=None):
        return NotImplemented
