def normalize_key(key):
    # type: (str) -> str
    """
    Normalize a given string as a key, following these rules:
        - Key always starts with forward slash.
        - Key never ends with forward slash.
        - Duplicate forward slashes are reduced to a single forward slash.
    :param key: Key to normalize.
    :return: Normalized key.
    """
    assert isinstance(key, str), "Key must be a string."

    key = key.strip()
    if not key.startswith("/"):
        key = "/" + key

    if key.endswith("/"):
        key = key[:-1]

    while '//' in key:
        key = key.replace('//', '/')

    return key


def construct_key(*args):
    """
    Construct a key from a list of strings.
    Strings are joined with a forward slash and then normalized.

    :param args: List of strings.
    :return: New key.
    """
    for arg in args:
        assert isinstance(arg, str), "All key parts must be strings."

    key = "/".join(args)
    return normalize_key(key)
