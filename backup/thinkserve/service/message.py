from typing import TypeVar, Generic
from abc import abstractmethod
from pydantic import BaseModel

class ServeContext(BaseModel):
    ...

class Message(BaseModel):
    ...
    
class StreamMessage(BaseModel):
    ...

_MT = TypeVar('_MT', bound=Message)
_ST = TypeVar('_ST', bound=StreamMessage)
    
class SplittableMessage(Message, Generic[_ST]):
    @abstractmethod
    def split(self) -> _ST:
        ...
    
class JoinableStreamMessage(StreamMessage, Generic[_MT]):
    @classmethod
    @abstractmethod
    def Join(cls) -> _MT:
        ...
        
        
        
__all__ = [
    'ServeContext',
    'Message', 
    'StreamMessage', 
    'SplittableMessage', 
    'JoinableStreamMessage'
]