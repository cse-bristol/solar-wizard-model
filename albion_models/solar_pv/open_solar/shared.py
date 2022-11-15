"""
Shared functions etc to avoid circ refs
"""

import os


def is_newer(f1: str, f2: str) -> bool:
    """
    :return: True if f1 is newer than f2; False if f1 is not newer than f2 or f1 or f2 don't exist or are not files.
    """
    if not os.path.exists(f1) or not os.path.exists(f2) or not os.path.isfile(f1) or not os.path.isfile(f2):
        return False
    return os.path.getmtime(f1) > os.path.getmtime(f2)
