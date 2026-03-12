import os
import inspect
import logging

from functools import wraps
from pathlib import Path
from typing import (Callable, Any, Coroutine, Sequence, Literal, overload)
from contextvars import ContextVar

from starlette.staticfiles import StaticFiles
from starlette.websockets import WebSocket, WebSocketDisconnect
from fastapi import FastAPI

from reactpy.config import REACTPY_DEBUG_MODE
from reactpy.core.serve import serve_layout
from reactpy.core.layout import Layout as _Layout
from reactpy.core.hooks import ConnectionContext
from reactpy.core.types import VdomDict
from reactpy.backend._common import STREAM_PATH, PATH_PREFIX
from reactpy.backend.types import Connection, Location
from reactpy.backend.starlette import Options, _make_send_recv_callbacks, _setup_common_routes, _make_index_route

from .heads import default_head

from ._internal._types import (ComponentType, ComponentCreator, ComponentLiked, ComponentLikedCreator, Component)

_logger = logging.getLogger(__name__)

# region context vars
_is_rerender = ContextVar('_is_rerender', default=False)
_app = ContextVar('_app', default=None)
'''The current FastAPI app.'''

def is_rerender() -> bool:
    '''Returns whether the current render is a re-render.'''
    return _is_rerender.get()

@overload
def current_app()->FastAPI:...
@overload
def current_app(raise_err: Literal[True])->FastAPI:...
@overload
def current_app(raise_err: Literal[False])->FastAPI|None:...

def current_app(raise_err: bool=True):  # type: ignore
    '''
    Get the current running FastAPI app in this process. 
    Raises an error if no app is found and `raise_err` is `True`.
    '''
    app = _app.get()
    if app is None and raise_err:
        raise RuntimeError("No FastAPI app found")
    return app

class Layout(_Layout):
    async def _render_component(
        self, exit_stack, old_state, new_state, component: ComponentType,
    ) -> None:
        if old_state is None:
            _is_rerender.set(False)
        else:
            _is_rerender.set(True)
        life_cycle_state = new_state.life_cycle_state
        life_cycle_hook = life_cycle_state.hook
        self._model_states_by_life_cycle_state_id[life_cycle_state.id] = new_state
        
        await life_cycle_hook.affect_component_will_render(component)   # type: ignore
        exit_stack.push_async_callback(life_cycle_hook.affect_layout_did_render)
        try:
            raw_model = component.render()  # type: ignore
            if isinstance(raw_model, Coroutine):
                raw_model = await raw_model
            if raw_model:
                children = [raw_model]
            else:
                children = []
            wrapper_model: VdomDict = {"tagName": "", "children": children}
            await self._render_model(exit_stack, old_state, new_state, wrapper_model)
        except Exception as error:
            _logger.exception(f"Failed to render {component}")
            new_state.model.current = { # type: ignore
                "tagName": "",
                "error": (
                    f"{type(error).__name__}: {error}"
                    if REACTPY_DEBUG_MODE.current
                    else ""
                ),
            }
        finally:
            await life_cycle_hook.affect_component_did_render()

        try:
            parent = new_state.parent
        except AttributeError:
            pass  # only happens for root component
        else:
            key, index = new_state.key, new_state.index
            parent.children_by_key[key] = new_state
            # need to add this model to parent's children without mutating parent model
            old_parent_model = parent.model.current
            old_parent_children = old_parent_model["children"]  # type: ignore
            parent.model.current = {    # type: ignore
                **old_parent_model,
                "children": [
                    *old_parent_children[:index],
                    new_state.model.current,
                    *old_parent_children[index + 1 :],
                ],
            }

__all__ = [
    "is_rerender",
    "current_app",
    "ComponentType",
    "ComponentCreator",
    "Component",
    "ComponentLiked",
    "ComponentLikedCreator",
    "Layout",
]
# endregion types

# region methods
_pages: list[tuple[str, ComponentCreator]] = []
_current_dir = Path(os.path.dirname(os.path.abspath(__file__)))

RESOURCES_PATH = _current_dir / "resources"
'''local directory for common resources'''
RESOURCES_URL = PATH_PREFIX / "resources"
'''url for common resources(for client side, in HTML)'''

def _match_page(path: str) -> tuple[str, ComponentCreator]|None:
    path = '/' + path.rstrip("/").lstrip('/') + '/'
    for prefix, page in _pages:
        if path.startswith(prefix):
            return prefix, page
    return None

def _init_app(app: FastAPI):
    _app.set(app)   # type: ignore
    if getattr(app, "__reactpy_initialized__", False):
        return
    _logger.debug(f"Initializing ReactPy app {app!r}...")
    _setup_common_routes(Options(serve_index_route=False), app)
    
    # add common resources
    app.mount(
        str(RESOURCES_URL),
        StaticFiles(directory=str(RESOURCES_PATH), check_dir=False),
    )
    
    async def model_stream(socket: WebSocket) -> None:
        await socket.accept()
        send, recv = _make_send_recv_callbacks(socket)

        pathname = "/" + socket.scope["path_params"].get("path", "")
        matched_page = _match_page(pathname)
        if not matched_page:
            await socket.close(code=1000)
            return
        prefix, component = matched_page
        pathname = pathname[len(prefix) :] or "/"
        search = socket.scope["query_string"].decode()
    
        try:
            comp = component()
            if isinstance(comp, Coroutine):
                comp = await comp
            await serve_layout(
                Layout( # type: ignore
                    ConnectionContext(
                        comp,
                        value=Connection(
                            scope=socket.scope,
                            location=Location(pathname, f"?{search}" if search else ""),
                            carrier=socket,
                        ),
                    )
                ),
                send,
                recv,
            )
        except BaseExceptionGroup as egroup:
            for e in egroup.exceptions:
                if isinstance(e, WebSocketDisconnect):
                    _logger.info(f"WebSocket disconnect: {e.code}")
                    break
            else:  # nocov
                raise

    app.add_websocket_route(str(STREAM_PATH), model_stream)
    app.add_websocket_route(f"{STREAM_PATH}/{{path:path}}", model_stream)
    setattr(app, "__reactpy_initialized__", True)

def add_page(
    app: FastAPI,
    component: ComponentCreator,
    path: str='/',
    head: Sequence[VdomDict] | VdomDict | str |None = None,
):
    '''
    Register a component as a page on the given Starlette/FastAPI app.
    Parameters:
        app: The Starlette/FastAPI app to register the page on.
        component: The component constructor for the page.
        path: The URL path for the page. By default, the root path '/' is used(index page).
        head: `<head>` elements to include in the page. By default it is:
            ```
            <head>
                <title>{app.title}</title>
                <link rel="icon" href="/_reactpy/assets/reactpy-logo.ico" type="image/x-icon" />
                <meta name="viewport" content="width=device-width, initial-scale=1" />
                <meta charset="UTF-8" />
                <script src="https://cdn.tailwindcss.com"></script>
            </head>
            ```
            ```
    '''
    _init_app(app)
    
    path = '/' + path.rstrip("/").lstrip('/')
    curr = _match_page(path)
    if curr is not None:
        _logger.warning(f"Overriding existing page at path {path!r} (was {_match_page(path)!r})")
        curr_path, _ = curr
        for i, (p, _) in enumerate(_pages):
            if p == curr_path:
                del _pages[i]
                break
        
    _pages.append((path + '/', component))
    _pages.sort(key=lambda x: -len(x[0]))
    
    if not head:
        head = default_head(app)
    
    if curr is None:
        index_route = _make_index_route(Options(head=head))
        app.add_route(f"{path}", index_route)
        app.add_route(f"{path}/", index_route)
        app.add_route(path + "/{path:path}", index_route)

def component(
    function: ComponentLikedCreator
) -> Callable[..., Component]:
    """A decorator for defining a new component.

    Parameters:
        function: The component's :meth:`reactpy.core.proto.ComponentType.render` function.
    """
    if hasattr(function, 'render'):
        # already a component
        return function  # type: ignore
    
    sig = inspect.signature(function)

    if "key" in sig.parameters and sig.parameters["key"].kind in (
        inspect.Parameter.KEYWORD_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        msg = f"Component render function {function} uses reserved parameter 'key'"
        raise TypeError(msg)
    
    @wraps(function)
    def constructor(*args: Any, key: Any | None = None, **kwargs: Any) -> Component:
        return Component(function, key, args, kwargs, sig)

    return constructor
    
    
__all__.extend([
    "RESOURCES_PATH",
    "RESOURCES_URL",
    "add_page",
    "component",
])
# endregion methods