def quote_columns(columns):
    return list(map(lambda col: f"\"{col}\"", columns))


def dict_safe_get(d, parent_key, child_key):
    return d[parent_key] and d[parent_key][child_key]
