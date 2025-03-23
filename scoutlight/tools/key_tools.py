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

    # Trim leading/trailing spaces. Valid key does not start or end with spaces.
    key = key.strip()

    # Valid key must always start with forward slash.
    if not key.startswith("/"):
        key = "/" + key

    # Key must never end with forward slash.
    while key.endswith("/"):
        key = key[:-1]

    # Remove any duplicate forward slashes in the key.
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
        assert isinstance(arg, basestring), "All key parts must be strings."

    key = "/".join(args)
    return normalize_key(key)


def starts_with(key, sub_key):
    """
    Test if key starts with sub_key.

    :param key: Key to test.
    :param sub_key: Subkey to test.
    :return: True if key starts with sub_key, otherwise False.
    """
    assert isinstance(key, str) and len(str) > 0, "Key must be a string."
    assert isinstance(sub_key, str), "Subkey must be a string."

    if not sub_key.endswith("/"):
        sub_key += '/'

    return key.startswith(sub_key)
