import inspect

from abc import ABC, abstractmethod
from typing import (Awaitable, Callable, Union, TYPE_CHECKING, TypeAliasType, TypeVar, Protocol, 
                    runtime_checkable, Any, Coroutine)
from reactpy.core.component import Component as _Component
from reactpy.core.types import ComponentType as ReactPyComponentType, VdomDict

if TYPE_CHECKING:
    from ..components.base import AdvancedComponent

_T = TypeVar("_T")
_SyncOrAsyncCallable = TypeAliasType('_SyncOrAsyncCallable', Callable[..., _T] | Callable[..., Awaitable[_T]], type_params=(_T,))

@runtime_checkable
class AsyncComponentType(Protocol):
    key: str | int | None
    type: Any
    
    async def render(self) -> "ComponentLiked":
        """Render the component's view model."""
        
class _AdvanceComponentBase(ABC):
    @abstractmethod
    def render(self): ...

ComponentType = Union[ReactPyComponentType, AsyncComponentType, "AdvancedComponent", 'Component']
ComponentCreator = _SyncOrAsyncCallable[ComponentType]

ComponentLiked = ComponentType | VdomDict | str | None
ComponentLikedCreator = _SyncOrAsyncCallable[ComponentLiked]

class Component(_Component):
    type: ComponentLikedCreator
    key: Any | None
    
    def __init__(
        self,
        function: ComponentLikedCreator,
        key: Any | None,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        sig: inspect.Signature,
    ) -> None:
        super().__init__(function, key, args, kwargs, sig)  # type: ignore
        
    async def render(self) -> ComponentLiked:
        r = self.type(*self._args, **self._kwargs)
        if isinstance(r, Coroutine):
            r = await r
        if isinstance(r, _AdvanceComponentBase):
            r = r.render()
        if isinstance(r, Coroutine):
            r = await r
        return r    # type: ignore

__all__ = [
    "AsyncComponentType",
    "ComponentType",
    "ComponentCreator",
    "ComponentLiked",
    "ComponentLikedCreator",
    "Component",
]