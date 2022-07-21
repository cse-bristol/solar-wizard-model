Easting = int
Northing = int

_500KM = 500000
_100KM = 100000
_10KM = 10000
_1KM = 1000


def _get_1st_letter(easting: Easting, northing: Northing) -> str:
    easting = _round_down_to(easting, _500KM)
    northing = _round_down_to(northing, _500KM)
    return {
        (0, 0): 'S',
        (_500KM, 0): 'T',
        (0, _500KM): 'N',
        (_500KM, _500KM): 'O',
        (0, _500KM * 2): 'H',
    }[(easting, northing)]


def _get_2nd_letter(easting: Easting, northing: Northing) -> str:
    # How many 100kms above the nearest multiple of 500km the easting/northing are:
    easting = _round_down_to(easting % _500KM, _100KM) // _100KM
    northing = _round_down_to(northing % _500KM, _100KM) // _100KM
    square_id = 20 - (northing * 5) + easting
    letter = chr(ord('A') + square_id)
    # 2nd letter in OS national grid includes all letters except I,
    # to form a 5x5 grid:
    if letter >= 'I':
        square_id += 1
        letter = chr(ord('A') + square_id)
    return letter


def _get_quadrant(easting: Easting, northing: Northing) -> str:
    # Which quadrant within a 10km square the easting/northing are in
    easting = _round_down_to(easting % _10KM, 5000)
    northing = _round_down_to(northing % _10KM, 5000)
    return {
        (0, 0): "sw",
        (0, 5000): "nw",
        (5000, 0): "se",
        (5000, 5000): "ne",
    }[(easting, northing)]


def _round_down_to(num: int, divisor: int):
    """Round down to the nearest `divisor`"""
    return num - (num % divisor)


def en_to_lidar_zip_id(easting: Easting, northing: Northing) -> str:
    """
    Convert an easting/northing pair to a string like SV54ne, which is the format
    used to identify LiDAR zip files.
    """
    letter_1 = _get_1st_letter(easting, northing)
    letter_2 = _get_2nd_letter(easting, northing)
    # How many 10kms above the nearest multiple of 100km the easting/northing are:
    numbers = str(_round_down_to(easting % _100KM, _10KM) // _10KM) + \
        str(_round_down_to(northing % _100KM, _10KM) // _10KM)
    corner = _get_quadrant(easting, northing)
    return letter_1 + letter_2 + numbers + corner


def en_to_welsh_lidar_zip_id(easting: Easting, northing: Northing) -> str:
    """
    Convert an easting/northing pair to a string like SM72, which is the format
    used to identify Welsh LiDAR zip files.
    """
    letter_1 = _get_1st_letter(easting, northing)
    letter_2 = _get_2nd_letter(easting, northing)
    # How many 10kms above the nearest multiple of 100km the easting/northing are:
    numbers = str(_round_down_to(easting % _100KM, _10KM) // _10KM) + \
        str(_round_down_to(northing % _100KM, _10KM) // _10KM)
    return letter_1 + letter_2 + numbers