import json


def dump_as_ascii_string(dict_obj):
    return json.dumps(dict_obj, ensure_ascii=True)
