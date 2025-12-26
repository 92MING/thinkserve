import inspect
import asyncio

from functools import wraps
from typing import (Awaitable, Coroutine, Any, TypedDict, overload, Unpack, Callable,
                    TypeAliasType, TypeVar, ClassVar, _TypedDictMeta)   # type: ignore
from reactpy.core.vdom import separate_attributes_and_children, separate_attributes_and_event_handlers

from .._internal._types import ComponentLiked as _ComponentLiked, _AdvanceComponentBase
from ..basic import is_rerender

# region event types
class ElementTarget(TypedDict):
    tagName: str
    boundingClientRect: dict[str, float]
    value: str
    checked: bool
    files: list[Any]
    id: str
    className: str

class HandlerEvent(TypedDict):
    """Base event interface for all DOM events"""
    bubbles: bool
    composed: bool
    currentTarget: ElementTarget
    defaultPrevented: bool
    eventPhase: int
    isTrusted: bool
    target: ElementTarget
    timeStamp: float
    type: str
    
class MouseEvent(HandlerEvent):
    """Mouse event interface"""
    selection: str | None
    altKey: bool
    button: int
    buttons: int
    clientX: float
    clientY: float
    ctrlKey: bool
    metaKey: bool
    pageX: float
    pageY: float
    screenX: float
    screenY: float
    shiftKey: bool
    movementX: float
    movementY: float
    offsetX: float
    offsetY: float
    x: float
    y: float
    relatedTarget: ElementTarget | None

class KeyboardEvent(HandlerEvent):
    """Keyboard event interface"""
    altKey: bool
    code: str
    ctrlKey: bool
    key: str
    keyCode: int
    location: int
    metaKey: bool
    repeat: bool
    shiftKey: bool

class FocusEvent(HandlerEvent):
    """Focus event interface"""
    relatedTarget: ElementTarget | None

class InputEvent(HandlerEvent):
    """Input event interface"""
    data: str | None
    inputType: str
    isComposing: bool

class ChangeEvent(HandlerEvent):
    """Change event interface"""
    value: Any

class FormEvent(HandlerEvent):
    """Form event interface"""
    pass

class DragEvent(MouseEvent):
    """Drag event interface"""
    dataTransfer: dict[str, Any] | None

class TouchEvent(HandlerEvent):
    """Touch event interface"""
    altKey: bool
    ctrlKey: bool
    metaKey: bool
    shiftKey: bool
    touches: list[dict[str, Any]]
    targetTouches: list[dict[str, Any]]
    changedTouches: list[dict[str, Any]]

class WheelEvent(MouseEvent):
    """Wheel event interface"""
    deltaX: float
    deltaY: float
    deltaZ: float
    deltaMode: int

class ClipboardEvent(HandlerEvent):
    """Clipboard event interface"""
    clipboardData: dict[str, Any] | None

class AnimationEvent(HandlerEvent):
    """Animation event interface"""
    animationName: str
    elapsedTime: float

class TransitionEvent(HandlerEvent):
    """Transition event interface"""
    propertyName: str
    elapsedTime: float

class MediaEvent(HandlerEvent):
    """Media event interface"""
    currentTime: float
    duration: float
    paused: bool
    volume: float

class ScrollEvent(HandlerEvent):
    """Scroll event interface"""
    scrollTop: float
    scrollLeft: float
    scrollHeight: float
    scrollWidth: float

class ResizeEvent(HandlerEvent):
    """Resize event interface"""
    width: float
    height: float

__all__ = [
    "HandlerEvent",
    "MouseEvent",
    "KeyboardEvent",
    "FocusEvent",
    "InputEvent",
    "ChangeEvent",
    "FormEvent",
    "DragEvent",
    "TouchEvent",
    "WheelEvent",
    "ClipboardEvent",
    "AnimationEvent",
    "TransitionEvent",
    "MediaEvent",
    "ScrollEvent",
    "ResizeEvent"
]
# endregion

_T = TypeVar('_T', bound=HandlerEvent)
_EventHandler = TypeAliasType("_EventHandler", Callable[[_T], Any|Awaitable[Any]]|Callable[[], Any|Awaitable[Any]], type_params=(_T,))

class _SafeTypeDictMeta(_TypedDictMeta):
    def __new__(cls, name, bases, attrs, **kwargs):
        kwargs.pop('extra_items', None)
        return super().__new__(cls, name, bases, attrs, **kwargs)

class ComponentAttributes(TypedDict, total=False, extra_items=Any, metaclass=_SafeTypeDictMeta):    # type: ignore
    key: Any
    '''a unique identifier among sibling components'''
    
    # HTML Global Attributes
    accesskey: str
    '''Specifies a shortcut key to activate/focus an element'''
    autocapitalize: str
    '''Controls whether and how text input is automatically capitalized'''
    autofocus: bool
    '''Specifies that the element should automatically get focus when the page loads'''
    class_: str
    '''Specifies one or more classnames for an element (refers to a class in a style sheet)'''
    className: str
    '''Alternative to class_ for React compatibility'''
    contenteditable: str | bool
    '''Specifies whether the content of an element is editable or not'''
    contextmenu: str
    '''Specifies a context menu for an element'''
    data_: dict[str, Any]
    '''Used to store custom data private to the page or application'''
    dir: str
    '''Specifies the text direction for the content in an element'''
    draggable: str | bool
    '''Specifies whether an element is draggable or not'''
    dropzone: str
    '''Specifies whether the dragged data is copied, moved, or linked, when dropped'''
    hidden: bool
    '''Specifies that an element is not yet, or is no longer, relevant'''
    id: str
    '''Specifies a unique id for an element'''
    inputmode: str
    '''Specifies the type of data that might be entered by the user while editing the element or its contents'''
    is_: str
    '''Allows you to specify that a standard HTML element should behave like a registered custom built-in element'''
    itemid: str
    '''Specifies the unique, global identifier of an item'''
    itemprop: str
    '''Specifies properties of an item'''
    itemref: str
    '''Specifies properties that are not descendants of an element with the itemscope attribute'''
    itemscope: bool
    '''Specifies that an element has associated metadata'''
    itemtype: str
    '''Specifies the URL of the vocabulary that will be used to define itemprop's in the data structure'''
    lang: str
    '''Specifies the language of the element's content'''
    part: str
    '''Indicates which part of a shadow tree an element belongs to'''
    role: str
    '''Defines an element's meaning or purpose'''
    slot: str
    '''Specifies which slot in a shadow DOM a element should be inserted into'''
    spellcheck: str | bool
    '''Specifies whether the element is to have its spelling and grammar checked or not'''
    style: str | dict[str, Any]
    '''Specifies an inline CSS style for an element'''
    tabindex: int
    '''Specifies the tabbing order of an element'''
    title: str
    '''Specifies extra information about an element'''
    translate: str | bool
    '''Specifies whether the content of an element should be translated or not'''
    
    # ARIA Attributes
    aria_label: str
    '''Defines a string value that labels the current element'''
    aria_labelledby: str
    '''Identifies the element that labels the current element'''
    aria_describedby: str
    '''Identifies the element that describes the current element'''
    aria_expanded: bool
    '''Indicates if the element is expanded'''
    aria_hidden: bool
    '''Indicates that the element is not visible'''
    aria_live: str
    '''Indicates that an element will be updated'''
    aria_atomic: bool
    '''Indicates whether the entire region should be read when changes occur'''
    aria_busy: bool
    '''Indicates whether an element is being modified'''
    aria_controls: str
    '''Identifies the element whose contents are controlled by the current element'''
    aria_current: str
    '''Indicates the element that represents the current item'''
    aria_details: str
    '''Identifies the element that provides a detailed description'''
    aria_disabled: bool
    '''Indicates that the element is perceivable but disabled'''
    aria_dropeffect: str
    '''Indicates what functions can be performed when a dragged object is released'''
    aria_errormessage: str
    '''Identifies the element that provides an error message'''
    aria_flowto: str
    '''Identifies the next element in an alternate reading order'''
    aria_grabbed: bool
    '''Indicates an element's "grabbed" state in a drag-and-drop operation'''
    aria_haspopup: str | bool
    '''Indicates the availability and type of interactive popup element'''
    aria_invalid: str | bool
    '''Indicates the entered value does not conform to the format expected'''
    aria_keyshortcuts: str
    '''Indicates keyboard shortcuts that an author has implemented to activate or give focus to an element'''
    aria_owns: str
    '''Identifies an element to establish a parent/child relationship'''
    aria_relevant: str
    '''Indicates what notifications the user agent will trigger'''
    aria_roledescription: str
    '''Defines a human-readable description for the role of an element'''
    
    # Event Handlers - Mouse Events
    onclick: _EventHandler[MouseEvent]
    '''Fires on a mouse click on the element'''
    oncontextmenu: _EventHandler[MouseEvent]
    '''Script to be run when a context menu is triggered'''
    ondblclick: _EventHandler[MouseEvent]
    '''Fires on a mouse double-click on the element'''
    onmousedown: _EventHandler[MouseEvent]
    '''Fires when a mouse button is pressed down on an element'''
    onmouseenter: _EventHandler[MouseEvent]
    '''Fires when the mouse pointer enters an element'''
    onmouseleave: _EventHandler[MouseEvent]
    '''Fires when the mouse pointer leaves an element'''
    onmousemove: _EventHandler[MouseEvent]
    '''Fires when the mouse pointer is moving while it is over an element'''
    onmouseover: _EventHandler[MouseEvent]
    '''Fires when the mouse pointer moves over an element'''
    onmouseout: _EventHandler[MouseEvent]
    '''Fires when a user moves the mouse pointer out of an element'''
    onmouseup: _EventHandler[MouseEvent]
    '''Fires when a mouse button is released over an element'''
    
    # Event Handlers - Keyboard Events
    onkeydown: _EventHandler[KeyboardEvent]
    '''Fires when a user is pressing a key'''
    onkeypress: _EventHandler[KeyboardEvent]
    '''Fires when a user presses a key'''
    onkeyup: _EventHandler[KeyboardEvent]
    '''Fires when a user releases a key'''
    
    # Event Handlers - Frame/Object Events
    onabort: _EventHandler[HandlerEvent]
    '''Script to be run on abort'''
    onbeforeunload: _EventHandler[HandlerEvent]
    '''Script to be run when the document is about to be unloaded'''
    onerror: _EventHandler[HandlerEvent]
    '''Script to be run when an error occurs'''
    onhashchange: _EventHandler[HandlerEvent]
    '''Script to be run when there has been changes to the anchor part of the a URL'''
    onload: _EventHandler[HandlerEvent]
    '''Fires after the page is finished loading'''
    onpageshow: _EventHandler[HandlerEvent]
    '''Script to be run when a user navigates to a page'''
    onpagehide: _EventHandler[HandlerEvent]
    '''Script to be run when a user navigates away from a page'''
    onresize: _EventHandler[ResizeEvent]
    '''Fires when the browser window is resized'''
    onscroll: _EventHandler[ScrollEvent]
    '''Script to be run when an element's scrollbar is being scrolled'''
    onunload: _EventHandler[HandlerEvent]
    '''Fires once a page has unloaded'''
    
    # Event Handlers - Form Events
    onblur: _EventHandler[FocusEvent]
    '''Fires the moment that the element loses focus'''
    onchange: _EventHandler[ChangeEvent]
    '''Fires the moment when the value of the element is changed'''
    onfocus: _EventHandler[FocusEvent]
    '''Fires the moment when the element gets focus'''
    onfocusin: _EventHandler[FocusEvent]
    '''Script to be run when an element is about to get focus'''
    onfocusout: _EventHandler[FocusEvent]
    '''Script to be run when an element is about to lose focus'''
    oninput: _EventHandler[InputEvent]
    '''Script to be run when an element gets user input'''
    oninvalid: _EventHandler[HandlerEvent]
    '''Script to be run when an element is invalid'''
    onreset: _EventHandler[FormEvent]
    '''Fires when the Reset button in a form is clicked'''
    onsearch: _EventHandler[InputEvent]
    '''Fires when the user writes something in a search field'''
    onselect: _EventHandler[HandlerEvent]
    '''Fires after some text has been selected in an element'''
    onsubmit: _EventHandler[FormEvent]
    '''Fires when a form is submitted'''
    
    # Event Handlers - Drag Events
    ondrag: _EventHandler[DragEvent]
    '''Script to be run when an element is dragged'''
    ondragend: _EventHandler[DragEvent]
    '''Script to be run at the end of a drag operation'''
    ondragenter: _EventHandler[DragEvent]
    '''Script to be run when an element has been dragged to a valid drop target'''
    ondragleave: _EventHandler[DragEvent]
    '''Script to be run when an element leaves a valid drop target'''
    ondragover: _EventHandler[DragEvent]
    '''Script to be run when an element is being dragged over a valid drop target'''
    ondragstart: _EventHandler[DragEvent]
    '''Script to be run at the start of a drag operation'''
    ondrop: _EventHandler[DragEvent]
    '''Script to be run when dragged element is being dropped'''
    
    # Event Handlers - Clipboard Events
    oncopy: _EventHandler[ClipboardEvent]
    '''Fires when the user copies the content of an element'''
    oncut: _EventHandler[ClipboardEvent]
    '''Fires when the user cuts the content of an element'''
    onpaste: _EventHandler[ClipboardEvent]
    '''Fires when the user pastes some content in an element'''
    
    # Event Handlers - Media Events
    oncanplay: _EventHandler[MediaEvent]
    '''Script to be run when a file is ready to start playing'''
    oncanplaythrough: _EventHandler[MediaEvent]
    '''Script to be run when a file can be played all the way to the end without pausing'''
    ondurationchange: _EventHandler[MediaEvent]
    '''Script to be run when the length of the media changes'''
    onemptied: _EventHandler[MediaEvent]
    '''Script to be run when something bad happens and the file is suddenly unavailable'''
    onended: _EventHandler[MediaEvent]
    '''Script to be run when the media has reach the end'''
    onloadeddata: _EventHandler[MediaEvent]
    '''Script to be run when media data is loaded'''
    onloadedmetadata: _EventHandler[MediaEvent]
    '''Script to be run when meta data are loaded'''
    onloadstart: _EventHandler[MediaEvent]
    '''Script to be run just as the file begins to load before anything is actually loaded'''
    onpause: _EventHandler[MediaEvent]
    '''Script to be run when the media is paused either by the user or programmatically'''
    onplay: _EventHandler[MediaEvent]
    '''Script to be run when the media is ready to start playing'''
    onplaying: _EventHandler[MediaEvent]
    '''Script to be run when the media actually has started playing'''
    onprogress: _EventHandler[MediaEvent]
    '''Script to be run when the browser is in the process of getting the media data'''
    onratechange: _EventHandler[MediaEvent]
    '''Script to be run each time the playback rate changes'''
    onseeked: _EventHandler[MediaEvent]
    '''Script to be run when the seeking attribute is set to false indicating that seeking has ended'''
    onseeking: _EventHandler[MediaEvent]
    '''Script to be run when the seeking attribute is set to true indicating that seeking is active'''
    onstalled: _EventHandler[MediaEvent]
    '''Script to be run when the browser is unable to fetch the media data for whatever reason'''
    onsuspend: _EventHandler[MediaEvent]
    '''Script to be run when fetching the media data is stopped before it is completely loaded for whatever reason'''
    ontimeupdate: _EventHandler[MediaEvent]
    '''Script to be run when the playing position has changed'''
    onvolumechange: _EventHandler[MediaEvent]
    '''Script to be run each time the volume is changed'''
    onwaiting: _EventHandler[MediaEvent]
    '''Script to be run when the media has paused but is expected to resume'''
    
    # Event Handlers - Touch Events (for mobile)
    ontouchcancel: _EventHandler[TouchEvent]
    '''Script to be run when the touch is interrupted'''
    ontouchend: _EventHandler[TouchEvent]
    '''Script to be run when a finger is removed from a touch screen'''
    ontouchmove: _EventHandler[TouchEvent]
    '''Script to be run when a finger is dragged across the screen'''
    ontouchstart: _EventHandler[TouchEvent]
    '''Script to be run when a finger is placed on a touch screen'''
    
    # Event Handlers - Animation Events
    onanimationend: _EventHandler[AnimationEvent]
    '''Script to be run when a CSS animation has completed'''
    onanimationiteration: _EventHandler[AnimationEvent]
    '''Script to be run when a CSS animation is repeated'''
    onanimationstart: _EventHandler[AnimationEvent]
    '''Script to be run when a CSS animation has started'''
    
    # Event Handlers - Transition Events
    ontransitionend: _EventHandler[TransitionEvent]
    '''Script to be run when a CSS transition has completed'''
    
type ComponentType = "_ComponentLiked|AdvancedComponent"

def _wrap_no_arg_handler(handler):
    if callable(handler):
        if asyncio.iscoroutinefunction(handler):
            @wraps(handler)
            async def wrapped(event):   # type: ignore
                return await handler()
        else:
            @wraps(handler)
            def wrapped(event):
                return handler()
        return wrapped
    return handler

def _merge_attributes(defaults: ComponentAttributes|None, overrides: ComponentAttributes) -> ComponentAttributes:
    if not defaults:
        return overrides
    new = defaults.copy()
    for k, v in overrides.items():
        if k not in new:
            new[k] = v
        else:
            if k in ('class_', 'className', 'class'):
                new[k] = f"{v} {new[k]}"    # type: ignore
            elif k in ('style', 'data_'):
                if isinstance(new[k], dict) and isinstance(v, dict):    # type: ignore
                    new[k] = {**new[k], **v}    # type: ignore
                else:
                    new[k] = v  # type: ignore
            else:
                new[k] = v
    return new

def _tidy_render_result(r):
    if isinstance(r, dict):
        if (d:=r.get('attributes')) and isinstance(d, dict):
            if 'class_' in d or 'className' in d:
                r['attributes'] = AdvancedComponent.TidyAttributes(d)   # type: ignore
    return r

async def _render_coro(coro: Coroutine):
    r = await coro
    return _tidy_render_result(r)
    
class AdvancedComponent(_AdvanceComponentBase):
    '''
    Advance class for creating advanced components in the React-like framework.
    Each subclass must implement the `__call__` method(can be sync or async) 
    to define its rendering logic.

    NOTE: advanced components are heavily rely on tailwind, i.e. you must
    include `TailwindCSS`(from heads.py) in page(will be included by default) 
    '''
    
    DefaultAttributes: ClassVar[ComponentAttributes|None] = None
    '''
    default attributes for the component.
    If set, it will merged with the attributes provided during initialization.
    '''
    
    attributes: ComponentAttributes
    '''
    attributes of the component. See:
    - https://developer.mozilla.org/en-US/docs/Web/API/HTMLElement
    - https://reactpy.dev/docs/reference/html-attributes.html
    '''
    children: list[ComponentType]
    '''children of the component'''
    
    @overload
    def __init__(
        self,
        *children: ComponentType,
        **attributes: Unpack[ComponentAttributes]
    ): ...
    
    @overload
    def __init__(
        self,
        attribute: ComponentAttributes,
        *children: ComponentType,
        **attributes: Unpack[ComponentAttributes]
    ): ...
    
    def __init__(self, *args, **kwargs):
        attributes, children = separate_attributes_and_children(args)
        attributes.update(kwargs)
        self.attributes = _merge_attributes(self.DefaultAttributes, attributes)    # type: ignore
        self.children = children
        
        # init states
        from ..state import State, StateModel
        for cls_attr in dir(self.__class__):
            if cls_attr.startswith('__'):
                continue
            try:
                cls_attr_val = getattr(self.__class__, cls_attr)
            except:
                continue
            if isinstance(cls_attr_val, (State, StateModel)):
                cls_attr_val._init_state(self)
                
    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__()
    
    @property
    def type(self):
        return self.__class__
    
    def render(self):
        '''real render method, wrapping `__call__`'''
        r = self.__call__()
        if isinstance(r, Coroutine):
            return _render_coro(r)
        else:
            return _tidy_render_result(r)
    
    def is_rerender(self)->bool:
        return is_rerender()
        
    @classmethod
    def TidyAttributes(cls, attributes: ComponentAttributes) -> dict[str, Any]:
        '''
        Define how to dump attributes to string, before returning to frontend.
        Some field names should be converted, e.g., class_ -> class
        '''
        attributes = attributes.copy()
        classes = ''
        for k in ('class_', 'className', 'class'):
            if k in attributes:
                classes += f"{attributes[k]} "  # type: ignore
                del attributes[k]
        if classes:
            attributes['class'] = classes
        return attributes
    
    @staticmethod
    def IsRerender() -> bool:
        '''
        Check if the component is being re-rendered.
        This is useful for preventing re-assign the initial value
        for states.
        '''
        from ..basic import is_rerender
        return is_rerender()
    
    def __call__(self)-> "ComponentType|Awaitable[ComponentType]":
        '''
        Each AdvancedComponent must implement this method to return a ComponentLiked object.
        This method can be asynchronous if needed.
        '''
        attrs, handlers = separate_attributes_and_event_handlers(self.attributes)
        for k, h in tuple(handlers.items()):
            if callable(h):
                param_count = len(inspect.signature(h).parameters)
                if param_count == 0:
                    handlers[k] = _wrap_no_arg_handler(h)   # type: ignore
                
        model = {
            "tagName": "",
            "attributes": AdvancedComponent.TidyAttributes(attrs),   # type: ignore
            "eventHandlers": handlers,
            "children": self.children,
        }
        return model    # type: ignore
    

__all__.extend([
    "ComponentType",
    "ComponentAttributes",
    "AdvancedComponent"
])