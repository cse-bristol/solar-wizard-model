from typing import Any, Dict


def flatten(tree: Dict[str, Any]) -> Dict[str, any]:
    return _flatten(tree, {})


def _flatten(value, paths: Dict[str, any], sep: str = '_', prefix: str = None) -> Dict[str, any]:
    if not value:
        return paths
    elif isinstance(value, dict):
        for k, v in value.items():
            path = prefix + sep + k if prefix else k
            _flatten(v, paths, sep, path)
    elif isinstance(value, list):
        for i, v in enumerate(value):
            path = prefix + sep + str(i) if prefix else str(i)
            _flatten(v, paths, sep, path)
    else:
        paths[prefix] = value
    return paths
