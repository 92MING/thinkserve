import reactpy.core.vdom

from typing import TYPE_CHECKING

if not TYPE_CHECKING:
    from warnings import warn
    from typing import Coroutine
    from reactpy.core.vdom import _is_attributes, separate_attributes_and_children, separate_attributes_and_event_handlers
    from ._internal.async_runner import wait_coro_in_sync

    def _tidy_attrs(attrs: dict[str, any]) -> dict[str, any]:
        keys = {k.lower().strip(): k for k in attrs.keys()}
        classes = ""
        for k in ('class_', 'class', 'classname', 'classes', '_class'):
            if (real_key:= keys.get(k)) is not None:
                classes += f"{attrs[real_key]} "
                del attrs[real_key]
        classes = classes.strip()
        if classes:
            attrs['class'] = classes
        return attrs

    def _vdom(tag: str, *attributes_and_children, **kwargs):
        if kwargs:
            if "key" in kwargs:
                if attributes_and_children:
                    maybe_attributes, *children = attributes_and_children
                    if _is_attributes(maybe_attributes):
                        attributes_and_children = (
                            {**maybe_attributes, "key": kwargs.pop("key")},
                            *children,
                        )
                    else:
                        attributes_and_children = (
                            {"key": kwargs.pop("key")},
                            maybe_attributes,
                            *children,
                        )
                else:
                    attributes_and_children = ({"key": kwargs.pop("key")},)
                warn(
                    "An element's 'key' must be declared in an attribute dict instead "
                    "of as a keyword argument. This will error in a future version.",
                    DeprecationWarning,
                )

            if kwargs:
                msg = f"Extra keyword arguments {kwargs}"
                raise ValueError(msg)

        model = {"tagName": tag}

        if not attributes_and_children:
            return model

        attributes, children = separate_attributes_and_children(attributes_and_children)
        key = attributes.pop("key", None)
        attributes, event_handlers = separate_attributes_and_event_handlers(attributes)

        if attributes:
            model["attributes"] = _tidy_attrs(attributes)    # type: ignore

        if children:
            from ._internal._types import _AdvanceComponentBase
            tidied = []
            for c in children:
                if isinstance(c, _AdvanceComponentBase):
                    c = c.render()
                if isinstance(c, Coroutine):
                    c = wait_coro_in_sync(c)
                tidied.append(c)
            model["children"] = tidied    # type: ignore
        if key is not None:
            model["key"] = key
        if event_handlers:
            model["eventHandlers"] = event_handlers # type: ignore
        return model

    reactpy.core.vdom.vdom = _vdom


from .heads import *  

from .basic import *

from .state import *