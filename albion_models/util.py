import os


def round_down_to(num: int, divisor: int):
    """Round down to the nearest `divisor`"""
    return int(num - (num % divisor))


def round_up_to(num: int, divisor: int):
    """Round up to the nearest `divisor`"""
    return int(num + divisor - (num % divisor))


def frange(start, end=None, step=None):
    """range() but accepts floats"""
    if end is None:
        end = start + 0.0
        start = 0.0

    if step is None:
        step = 1.0

    cur = float(start)

    while cur < end:
        yield cur
        cur += step


def get_cpu_count():
    return len(os.sched_getaffinity(0))
