import re
import os
import inspect
import logging

from metapensiero.pj.__main__ import transform_string
from reactpy.html import script
from typing import Callable
from types import ModuleType
from pathlib import Path
from weakref import ref
from functools import lru_cache

from .base import AdvancedComponent
from ..basic import is_rerender
from ..state import State


_logger = logging.getLogger(__name__)

@lru_cache
def _str_to_js(source: str):
    _logger.debug(f'Transforming `{source[:30]}...` to JS')
    return transform_string(source, enable_es6=True)

def _to_js(obj):
    source = inspect.getsource(obj)
    return _str_to_js(source)
    
    
class Script(AdvancedComponent):
    '''
    <script> component to include JavaScript code.
    You can pass a python function, class, module, file path, or raw JavaScript code as the script content.
    
    NOTE: no children/attributes are allowed in `Script` component(even `key`)
    '''
    
    _script_cache: str|None = None
    _old_script_ref: ref|None = None
    
    script = State[str|Callable|type|ModuleType|Path|None](None)
    
    def __init__(
        self,
        script: str|Callable|type|ModuleType|Path|None = None,
    ):
        super().__init__()
        if not is_rerender():
            self.script = script
    
    @property
    def _script_str(self)->str:
        if (curr := self.script) is None:
            return ''
        old = self._old_script_ref() if self._old_script_ref else None
        curr = self.script
        if curr == old and self._script_cache is not None:
            return self._script_cache
        if isinstance(curr, (str, Path)):
            if isinstance(curr, str):
                # check if it is a path
                if os.path.isfile(curr):
                    with open(curr, 'r', encoding='utf-8') as f:
                        curr = f.read()
            elif isinstance(curr, Path):
                if not os.path.isfile(curr):
                    raise FileNotFoundError(f"Script file not found: {curr}")
                with open(curr, 'r', encoding='utf-8') as f:
                    curr = f.read()
            # check if it is a js code or python code
            if re.search(r'\b(?<![\'"])(function|var|let|const|=>|export)(?![\'"])\b|console\.log.*;', curr):
                self._script_cache = curr
            else:
                self._script_cache = _str_to_js(curr)
        else:
            self._script_cache = _to_js(curr)
        return self._script_cache # type: ignore
            
    def __call__(self):
        return script(self._script_str)   # type: ignore


__all__ = ['Script']