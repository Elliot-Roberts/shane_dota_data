from typing import Iterable, TypeVar, Optional, Generator
import time

T = TypeVar('T')


def with_sleeps(iterator: Iterable[T], sleep_secs: float) -> T:
    """
    Generator that sleeps between yielding each element from an iterator.

    :param iterator: iterator to yield all items from
    :param sleep_secs: duration to sleep (in seconds) before yielding each element
    """
    for x in iterator:
        time.sleep(sleep_secs)
        yield x


def rate_limited_v1(iterable: Iterable[T], secs_between: float, prev_time: Optional[float] = 0) -> T:
    """
    Generator ensuring that items from an iterable are yielded with at least a specified time between.

    :param iterable: source of items
    :param secs_between: time between yields (in seconds)
    :param prev_time: optional time to treat as the most recent yield before the generator started
    """
    for x in iterable:
        now = time.time()
        time_since = now - prev_time
        if time_since < secs_between:
            time.sleep(secs_between - time_since)
        prev_time = time.time()
        yield x


def rate_limited_v2(generator: Generator[T], secs_between: float, prev_time: Optional[float] = 0) -> T:
    """
    Generator that yields items from a provided generator, ensuring that at least a specified time passes between the
    end of one call to `next()` the beginning of the next.

    :param generator: source of items
    :param secs_between: time between yields (in seconds)
    :param prev_time: optional time to treat as the most recent yield before the generator started
    """
    while True:
        now = time.time()
        time_since = now - prev_time
        if time_since < secs_between:
            time.sleep(secs_between - time_since)
        x = next(generator)
        prev_time = time.time()
        yield x
