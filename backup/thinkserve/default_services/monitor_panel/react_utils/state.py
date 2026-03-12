from dataclasses import dataclass
from random import choices
from reactpy import use_state
from pydantic import BaseModel
from typing import TypeVar, Generic, TYPE_CHECKING, Callable, overload, Self, ClassVar

if TYPE_CHECKING:
    from .components.base import AdvancedComponent

_T = TypeVar('_T')

def _random_state_id() -> str:
    return ''.join(choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=16))

@dataclass
class State(Generic[_T]):
    '''
    `State` is a special class which helps you manages states under an `AdvancedComponent`.
    Here is an example:
    ```python
    class MyButton(AdvancedComponent):
        text = State[str]("Click me")   # or `State("Click me")`

        def handle_click(self, event):
            current = self.text
            if current == "Click me":
                self.text = "Clicked!"
            else:
                self.text = "Click me"

        def __call__(self):
            return button(
                {"on_click": self.handle_click},
                self.text,
            )
    ```
    '''
    
    initial_value: _T|None = None
    '''Initial value of the state. If set, `initial_value_factory` will be ignored.'''
    initial_value_factory: Callable[[], _T]|None = None
    '''A factory function to produce the initial value.'''
    
    state_id: str = None    # type: ignore
    '''Unique identifier for the state. This value will be generated automatically in `__post_init__`.'''
    
    def __post_init__(self):
        if not self.state_id:
            self.state_id = _random_state_id()
            
    def _init_state(self, comp: "AdvancedComponent"):
        val, set_val = use_state(
            self.initial_value_factory()
            if self.initial_value_factory is not None
            else self.initial_value
        )
        setattr(comp, f'__state_value_{self.state_id}__', val)
        setattr(comp, f'__set_state_value_{self.state_id}__', set_val)
    
    @overload
    def __get__(self, instance: None, owner) -> Self: ...
    @overload
    def __get__(self, instance: "AdvancedComponent", owner) -> _T: ...
    
    def __get__(self, instance, owner):
        if instance is None:
            return self
        return getattr(instance, f'__state_value_{self.state_id}__')
    
    def __set__(self, instance: "AdvancedComponent|None", value: _T):
        set_val = getattr(instance, f'__set_state_value_{self.state_id}__')
        set_val(value)
        
class _StateModelGetWrapper:
    def __init__(self, comp: "AdvancedComponent", model: "StateModel", state_id: str):
        self.__comp = comp
        self.__state_id = state_id
        self.__model = model
        
    def __getattr__(self, name: str):
        if name in self.__model.__valid_state_fields__:  # type: ignore
            return getattr(self.__comp, f'__state_value_{self.__state_id}_{name}__')
        raise AttributeError(f"'{self.__model.__class__.__name__}' object has no attribute '{name}'")

    def __setattr__(self, name: str, value):
        if name in self.__model.__valid_state_fields__:  # type: ignore
            getattr(self.__comp, f'__set_state_value_{self.__state_id}_{name}__')(value)
        else:
            super().__setattr__(name, value)

class StateModel(BaseModel):
    '''
    Model for a group of states under an `AdvancedComponent`.
    NOTE: this class is a subclass of `pydantic.BaseModel`, so all features of Pydantic models are supported.
    
    Here is an example:
    ```python
    class MyStateModel(StateModel):    
        count: int = 0
        text: str = "Hello"
        
    class MyComponent(AdvancedComponent):
        state = MyStateModel()
        
        def handle_click(self, event):
            self.state.count += 1
            curr = self.state.text
            if curr == "Hello":
                self.state.text = "World"
            else:
                self.state.text = "Hello"
            
        def __call__(self):
            return div(
                {"on_click": self.handle_click},
                f"Count: {self.state.count}, Text: {self.state.text}"
            )
    '''
    
    __valid_state_fields__: ClassVar[set[str]]
    
    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs):
        valid_state_fields = set()
        for n, f in cls.model_fields.items():
            if (a:=f.alias) is not None:
                name = a
            else:
                name = n
            valid_state_fields.add(name)
        setattr(cls, '__valid_state_fields__', valid_state_fields)
        
    def _init_state(self, comp: "AdvancedComponent"):
        for name in self.__valid_state_fields__:
            val, set_val = use_state(getattr(self, name))
            setattr(comp, f'__state_value_{self.state_id}_{name}__', val)
            setattr(comp, f'__set_state_value_{self.state_id}_{name}__', set_val)
    
    def model_post_init(self, _) -> None:
        setattr(self, '__state_id__', _random_state_id())
    
    @property
    def state_id(self)-> str:
        return getattr(self, '__state_id__')
    
    def __get__(self, instance: "AdvancedComponent|None", owner) -> "StateModel|_StateModelGetWrapper":
        if instance is None:
            return self
        return _StateModelGetWrapper(instance, self, self.state_id)


type StateType = State | StateModel
    
    
__all__ = [
    'State',
    'StateModel',
    'StateType',
]
    