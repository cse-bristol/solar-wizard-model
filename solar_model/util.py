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


def esc_double_quotes(s: str) -> str:
    return s.replace('"', '\\"')


def is_newer(f1: str, f2: str) -> bool:
    """
    :return: True if f1 is newer than f2; False if f1 is not newer than f2 or f1 or f2 don't exist or are not files.
    """
    if not os.path.exists(f1) or not os.path.exists(f2) or not os.path.isfile(f1) or not os.path.isfile(f2):
        return False
    return os.path.getmtime(f1) > os.path.getmtime(f2)
