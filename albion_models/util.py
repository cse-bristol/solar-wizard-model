
def round_down_to(num: int, divisor: int):
    """Round down to the nearest `divisor`"""
    return int(num - (num % divisor))


def round_up_to(num: int, divisor: int):
    """Round up to the nearest `divisor`"""
    return int(num + divisor - (num % divisor))
