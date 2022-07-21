import re
from typing import Tuple, Dict

Easting = int
Northing = int
SquareSize = int

_500KM = 500000
_100KM = 100000
_10KM = 10000
_1KM = 1000

_1ST_LETTER: Dict[str, Tuple[Easting, Northing]] = {
    'S': (0, 0),
    'T': (_500KM, 0),
    'N': (0, _500KM),
    'O': (_500KM, _500KM),
    'H': (0, _500KM * 2),
}
"""
The 1st letter in an OS national grid ref (e.g the 'S' in 'SP2621').
This references the SW corner of a 500kmx500km square.
"""


def _2nd_letter(letter: str) -> Tuple[Easting, Northing]:
    """
    Handles the 2nd letter in an OS national grid string
    (e.g the 'P' in 'SP2621').

    This letter references the SW corner of a 100km x 100km square
    within the 500km x 500km square referenced by the 1st letter in
    the grid ref.

    Each of the 25 100km squares per 500km is represented by a letter
    of the alphabet, except 'I', with:
     * 'A' in the NE,
     * 'E' in the NW,
     * 'V' in the SE,
     * 'Z' in the SW.
    """
    letter = letter.upper()[0]
    idx = ord(letter) - ord('A')
    # 2nd letter in OS national grid includes all letters except I,
    # to form a 5x5 grid:
    if letter > 'I':
        idx -= 1

    easting = (idx % 5) * _100KM
    northing = (4 - (idx // 5)) * _100KM
    return easting, northing


def _rest(rest: str) -> Tuple[Easting, Northing, SquareSize]:
    rest = rest.upper()
    has_quadrant = rest[-2:] in ('SW', 'SE', 'NW', 'NE')
    digits = rest[:-2] if has_quadrant else rest
    quadrant = rest[-2:] if has_quadrant else None

    if len(digits) == 4:
        sq_size = 500 if has_quadrant else _1KM
        base = int(rest[0:2]) * _1KM, int(rest[2:4]) * _1KM, sq_size
        quad_size = 500
    elif len(digits) == 2:
        sq_size = 5000 if has_quadrant else _10KM
        base = int(rest[0]) * _10KM, int(rest[1]) * _10KM, sq_size
        quad_size = 5000
    else:
        raise ValueError(f"Unparseable rest of grid ref {rest}")

    if has_quadrant:
        if quadrant == 'SW':
            return base
        elif quadrant == 'SE':
            return base[0] + quad_size, base[1], base[2]
        elif quadrant == 'NW':
            return base[0], base[1] + quad_size, base[2]
        elif quadrant == 'NE':
            return base[0] + quad_size, base[1] + quad_size, base[2]
    else:
        return base

    raise ValueError(f"Unknown rest of grid ref {rest}")


def os_grid_ref_to_en(grid_ref: str) -> Tuple[Easting, Northing, SquareSize]:
    parsed = re.match('^([STNOH])([A-Z])([0-9]{2,4}(?:SE|SW|NE|NW)?)$', grid_ref.upper())
    if parsed is None:
        raise ValueError(f"Could not parse grid ref {grid_ref}")

    letter_1, letter_2, rest = parsed.groups()
    en_1 = _1ST_LETTER[letter_1]
    en_2 = _2nd_letter(letter_2)
    en_rest = _rest(rest)
    return en_1[0] + en_2[0] + en_rest[0], en_1[1] + en_2[1] + en_rest[1], en_rest[2]


def os_grid_ref_to_wkt(grid_ref: str) -> str:
    easting, northing, sq_size = os_grid_ref_to_en(grid_ref)
    return f'POLYGON(({easting} {northing}, ' \
           f'{easting} {northing + sq_size}, ' \
           f'{easting + sq_size} {northing + sq_size}, ' \
           f'{easting + sq_size} {northing}, ' \
           f'{easting} {northing}))'
