# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
from solar_model.util import round_down_to

Easting = int
Northing = int

_500KM = 500000
_100KM = 100000
_10KM = 10000
_1KM = 1000

_1ST_LETTER = {
    (0, 0): 'S',
    (_500KM, 0): 'T',
    (0, _500KM): 'N',
    (_500KM, _500KM): 'O',
    (0, _500KM * 2): 'H',
}


def is_in_range(easting: Easting, northing: Northing) -> bool:
    easting = round_down_to(easting, _500KM)
    northing = round_down_to(northing, _500KM)
    return _1ST_LETTER.get((easting, northing), None) is not None


def _get_1st_letter(easting: Easting, northing: Northing) -> str:
    easting = round_down_to(easting, _500KM)
    northing = round_down_to(northing, _500KM)
    return _1ST_LETTER[(easting, northing)]


def _get_2nd_letter(easting: Easting, northing: Northing) -> str:
    # How many 100kms above the nearest multiple of 500km the easting/northing are:
    easting = round_down_to(easting % _500KM, _100KM) // _100KM
    northing = round_down_to(northing % _500KM, _100KM) // _100KM
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
    easting = round_down_to(easting % _10KM, 5000)
    northing = round_down_to(northing % _10KM, 5000)
    return {
        (0, 0): "sw",
        (0, 5000): "nw",
        (5000, 0): "se",
        (5000, 5000): "ne",
    }[(easting, northing)]


def en_to_grid_ref(easting: Easting, northing: Northing, square_size: int) -> str:
    if not is_in_range(easting, northing):
        raise ValueError(f"easting and northing out of grid ref range: {easting}, {northing}")

    if square_size == _500KM:
        return _get_1st_letter(easting, northing)

    elif square_size == _100KM:
        letter_1 = _get_1st_letter(easting, northing)
        letter_2 = _get_2nd_letter(easting, northing)
        return letter_1 + letter_2

    elif square_size in (_10KM, 5000):
        letter_1 = _get_1st_letter(easting, northing)
        letter_2 = _get_2nd_letter(easting, northing)
        # How many 10kms above the nearest multiple of 100km the easting/northing are:
        numbers = str(round_down_to(easting % _100KM, _10KM) // _10KM) + \
                  str(round_down_to(northing % _100KM, _10KM) // _10KM)
        if square_size == 5000:
            corner = _get_quadrant(easting, northing)
            return letter_1 + letter_2 + numbers + corner
        return letter_1 + letter_2 + numbers

    else:
        raise ValueError(f"Unhandled square size: {square_size}")
