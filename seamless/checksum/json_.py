# pylint: disable=E1101

"""Seamless's implementation to convert from/to JSON
This is the basis of the "plain" celltype."""


import orjson


def json_encode(obj) -> str:
    """Encode as JSON, tolerating Numpy objects"""
    dump = orjson.dumps(obj, option=orjson.OPT_SERIALIZE_NUMPY)
    return dump.decode()


def json_dumps_bytes(obj) -> bytes:
    """Encode as JSON, with two-space identation and sorted keys, as bytes"""
    dump = orjson.dumps(obj, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS)
    return dump


def json_dumps(obj) -> str:
    """Encode as JSON, with two-space identation and sorted keys"""
    dump = json_dumps_bytes(obj)
    return dump.decode()
