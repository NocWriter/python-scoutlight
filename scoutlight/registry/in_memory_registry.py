import logging
from collections import OrderedDict

from typing import List, Dict, Union, Tuple, Optional

from scoutlight.registry import Registry, KeyDoesNotExist, Key
from scoutlight.tools.key_tools import normalize_key, starts_with

logger = logging.getLogger(__name__)


class InMemoryRegistry(Registry):

    def __init__(self):
        super(InMemoryRegistry, self).__init__()

        # Holds all key/value mappings.
        self._model = OrderedDict()  # type: Dict[Key, str]

    def _put(self, kv_list, conditional_key_exist=None):
        # type: (List[Tuple[Key, str]], Optional[Key, str]) -> bool
        # If we got conditional key, we need to check if it exists.
        if conditional_key_exist is not None and conditional_key_exist in self._model:
            return False

        for key, value in kv_list:
            self._model[key] = value

        return True

    def _get_one(self, get_key):
        # type: (Key) -> str
        if get_key not in self._model:
            raise KeyDoesNotExist("Key not found -- {}".format(get_key.key))

        return self._model[get_key]

    def _get(self, get_key, recursive=False, keep_order=False, keys_only=False, exclude_parent_keys=True):
        # type: (Key, bool, bool, bool, bool) -> Dict[str, str]

        result = OrderedDict() # type: Dict[str,str]
        for key, value in self._model.items():
            if keys_only:
                value = ''
            if not recursive and key.is_immediate_parent(get_key):
                result[key.key] = value
            elif recursive and key.is_a_parent(get_key):
                result[key.key] = value

        if exclude_parent_keys:
            result.pop(get_key.key, None)

        return result
