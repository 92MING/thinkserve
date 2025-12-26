from typing import Literal, Unpack, ClassVar, overload
from reactpy import html

from .base import AdvancedComponent, ComponentAttributes, ComponentType

type _Direction = Literal['row', 'row-reverse', 'column', 'column-reverse']
type _JustifyContent = Literal['start', 'end', 'center', 'space-between', 'space-around', 'space-evenly']
type _AlignItems = Literal['start', 'end', 'center', 'stretch', 'baseline']
type _FlexWrap = Literal['wrap', 'nowrap', 'wrap-reverse']
type _Gap = Literal['xs', 'sm', 'md', 'lg', 'xl', '2xl'] | int
type _Size = Literal['xs', 'sm', 'md', 'lg', 'xl'] | int | str


class Flex(AdvancedComponent):
    """Flexible layout component similar to Ant Design Flex"""
    
    DefaultAttributes: ClassVar[ComponentAttributes] = {
        'class_': 'flex'
    }
    
    direction: _Direction
    justify: _JustifyContent
    align: _AlignItems
    wrap: _FlexWrap
    gap: _Gap
    
    @overload
    def __init__(
        self,
        *children: ComponentType,
        direction: _Direction = 'row',
        justify: _JustifyContent = 'start',
        align: _AlignItems = 'start',
        wrap: _FlexWrap = 'nowrap',
        gap: _Gap = 'md',
        **attributes: Unpack[ComponentAttributes]
    ): ...
    
    @overload
    def __init__(
        self,
        attributes: ComponentAttributes,
        *children: ComponentType,
        direction: _Direction = 'row',
        justify: _JustifyContent = 'start',
        align: _AlignItems = 'start',
        wrap: _FlexWrap = 'nowrap',
        gap: _Gap = 'md',
        **kwargs: Unpack[ComponentAttributes]
    ): ...
    
    def __init__(self, *args, direction: _Direction = 'row', justify: _JustifyContent = 'start', align: _AlignItems = 'start', wrap: _FlexWrap = 'nowrap', gap: _Gap = 'md', **kwargs):
        self.direction = direction
        self.justify = justify
        self.align = align
        self.wrap = wrap
        self.gap = gap
        super().__init__(*args, **kwargs)
    
    def __call__(self):
        # Build CSS classes based on props
        classes = []
        
        # Direction mapping
        if self.direction == 'column':
            classes.append('flex-col')
        elif self.direction == 'column-reverse':
            classes.append('flex-col-reverse')
        elif self.direction == 'row-reverse':
            classes.append('flex-row-reverse')
        # 'row' is default, no need to add class
        
        # Justify content mapping
        justify_map = {
            'start': 'justify-start',
            'end': 'justify-end', 
            'center': 'justify-center',
            'space-between': 'justify-between',
            'space-around': 'justify-around',
            'space-evenly': 'justify-evenly'
        }
        if self.justify in justify_map:
            classes.append(justify_map[self.justify])
        
        # Align items mapping
        align_map = {
            'start': 'items-start',
            'end': 'items-end',
            'center': 'items-center', 
            'stretch': 'items-stretch',
            'baseline': 'items-baseline'
        }
        if self.align in align_map:
            classes.append(align_map[self.align])
        
        # Flex wrap mapping
        if self.wrap == 'wrap':
            classes.append('flex-wrap')
        elif self.wrap == 'wrap-reverse':
            classes.append('flex-wrap-reverse')
        # 'nowrap' is default, no need to add class
        
        # Handle gap
        if isinstance(self.gap, int):
            classes.append(f'gap-{self.gap}')
        elif self.gap in ['xs', 'sm', 'md', 'lg', 'xl', '2xl']:
            gap_map = {'xs': '1', 'sm': '2', 'md': '4', 'lg': '6', 'xl': '8', '2xl': '12'}
            classes.append(f'gap-{gap_map[self.gap]}')
        
        class_names = ' '.join(filter(None, classes))
        if class_names:
            self.attributes['class_'] = f"{self.attributes.get('class_', '')} {class_names}".strip()
        
        # Use TidyAttributes to process attributes before returning
        tidy_attrs = self.TidyAttributes(self.attributes)
        return html.div(tidy_attrs, *self.children)


class Grid(AdvancedComponent):
    """Grid layout component similar to Ant Design Grid"""
    
    DefaultAttributes: ClassVar[ComponentAttributes] = {
        'class_': 'grid'
    }
    
    cols: int | dict[str, int]
    gap: _Gap
    
    @overload
    def __init__(
        self,
        *children: ComponentType,
        cols: int | dict[str, int] = 12,
        gap: _Gap = 'md',
        **attributes: Unpack[ComponentAttributes]
    ): ...
    
    @overload
    def __init__(
        self,
        attributes: ComponentAttributes,
        *children: ComponentType,
        cols: int | dict[str, int] = 12,
        gap: _Gap = 'md',
        **kwargs: Unpack[ComponentAttributes]
    ): ...
    
    def __init__(self, *args, cols: int | dict[str, int] = 12, gap: _Gap = 'md', **kwargs):
        self.cols = cols
        self.gap = gap
        super().__init__(*args, **kwargs)
    
    def __call__(self):
        classes = []
        
        # Handle responsive columns
        if isinstance(self.cols, dict):
            for breakpoint, col_count in self.cols.items():
                if breakpoint == 'xs':
                    classes.append(f'grid-cols-{col_count}')
                else:
                    classes.append(f'{breakpoint}:grid-cols-{col_count}')
        else:
            classes.append(f'grid-cols-{self.cols}')
        
        # Handle gap
        if isinstance(self.gap, int):
            classes.append(f'gap-{self.gap}')
        elif self.gap in ['xs', 'sm', 'md', 'lg', 'xl', '2xl']:
            gap_map = {'xs': '1', 'sm': '2', 'md': '4', 'lg': '6', 'xl': '8', '2xl': '12'}
            classes.append(f'gap-{gap_map[self.gap]}')
        
        class_names = ' '.join(classes)
        if class_names:
            self.attributes['class_'] = f"{self.attributes.get('class_', '')} {class_names}".strip()
        
        tidy_attrs = self.TidyAttributes(self.attributes)
        return html.div(tidy_attrs, *self.children)


class Col(AdvancedComponent):
    """Grid column component"""
    
    DefaultAttributes: ClassVar[ComponentAttributes] = {
        'class_': ''
    }
    
    span: int | dict[str, int]
    offset: int | dict[str, int]
    
    @overload
    def __init__(
        self,
        *children: ComponentType,
        span: int | dict[str, int] = 1,
        offset: int | dict[str, int] = 0,
        **attributes: Unpack[ComponentAttributes]
    ): ...
    
    @overload
    def __init__(
        self,
        attributes: ComponentAttributes,
        *children: ComponentType,
        span: int | dict[str, int] = 1,
        offset: int | dict[str, int] = 0,
        **kwargs: Unpack[ComponentAttributes]
    ): ...
    
    def __init__(self, *args, span: int | dict[str, int] = 1, offset: int | dict[str, int] = 0, **kwargs):
        self.span = span
        self.offset = offset
        super().__init__(*args, **kwargs)
    
    def __call__(self):
        classes = []
        
        # Handle responsive span
        if isinstance(self.span, dict):
            for breakpoint, span_value in self.span.items():
                if breakpoint == 'xs':
                    classes.append(f'col-span-{span_value}')
                else:
                    classes.append(f'{breakpoint}:col-span-{span_value}')
        else:
            classes.append(f'col-span-{self.span}')
        
        # Handle responsive offset
        if isinstance(self.offset, dict):
            for breakpoint, offset_value in self.offset.items():
                if offset_value > 0:
                    if breakpoint == 'xs':
                        classes.append(f'col-start-{offset_value + 1}')
                    else:
                        classes.append(f'{breakpoint}:col-start-{offset_value + 1}')
        elif self.offset > 0:
            classes.append(f'col-start-{self.offset + 1}')
        
        class_names = ' '.join(classes)
        if class_names:
            self.attributes['class_'] = f"{self.attributes.get('class_', '')} {class_names}".strip()
        
        tidy_attrs = self.TidyAttributes(self.attributes)
        return html.div(tidy_attrs, *self.children)


class Layout(AdvancedComponent):
    """Main layout component"""
    
    DefaultAttributes: ClassVar[ComponentAttributes] = {
        'class_': 'min-h-screen flex flex-col'
    }
    
    @overload
    def __init__(
        self,
        *children: ComponentType,
        **attributes: Unpack[ComponentAttributes]
    ): ...
    
    @overload
    def __init__(
        self,
        attributes: ComponentAttributes,
        *children: ComponentType,
        **kwargs: Unpack[ComponentAttributes]
    ): ...
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def __call__(self):
        tidy_attrs = self.TidyAttributes(self.attributes)
        return html.div(tidy_attrs, *self.children)


class Header(AdvancedComponent):
    """Header layout component"""
    
    DefaultAttributes: ClassVar[ComponentAttributes] = {
        'class_': 'bg-white shadow-sm border-b border-gray-200 px-4 py-3'
    }
    
    height: int | str
    
    @overload
    def __init__(
        self,
        *children: ComponentType,
        height: int | str = 64,
        **attributes: Unpack[ComponentAttributes]
    ): ...
    
    @overload
    def __init__(
        self,
        attributes: ComponentAttributes,
        *children: ComponentType,
        height: int | str = 64,
        **kwargs: Unpack[ComponentAttributes]
    ): ...
    
    def __init__(self, *args, height: int | str = 64, **kwargs):
        self.height = height
        super().__init__(*args, **kwargs)
    
    def __call__(self):
        if isinstance(self.height, int):
            self.attributes['style'] = f"height: {self.height}px; {self.attributes.get('style', '')}"
        
        tidy_attrs = self.TidyAttributes(self.attributes)
        return html.header(tidy_attrs, *self.children)


class Footer(AdvancedComponent):
    """Footer layout component"""
    
    DefaultAttributes: ClassVar[ComponentAttributes] = {
        'class_': 'bg-gray-50 border-t border-gray-200 px-4 py-3 mt-auto'
    }
    
    @overload
    def __init__(
        self,
        *children: ComponentType,
        **attributes: Unpack[ComponentAttributes]
    ): ...
    
    @overload
    def __init__(
        self,
        attributes: ComponentAttributes,
        *children: ComponentType,
        **kwargs: Unpack[ComponentAttributes]
    ): ...
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def __call__(self):
        tidy_attrs = self.TidyAttributes(self.attributes)
        return html.footer(tidy_attrs, *self.children)


class Sider(AdvancedComponent):
    """Sidebar layout component"""
    
    DefaultAttributes: ClassVar[ComponentAttributes] = {
        'class_': 'bg-white border-r border-gray-200 flex-shrink-0 transition-all duration-300'
    }
    
    width: int | str
    collapsed: bool
    
    @overload
    def __init__(
        self,
        *children: ComponentType,
        width: int | str = 256,
        collapsed: bool = False,
        **attributes: Unpack[ComponentAttributes]
    ): ...
    
    @overload
    def __init__(
        self,
        attributes: ComponentAttributes,
        *children: ComponentType,
        width: int | str = 256,
        collapsed: bool = False,
        **kwargs: Unpack[ComponentAttributes]
    ): ...
    
    def __init__(self, *args, width: int | str = 256, collapsed: bool = False, **kwargs):
        self.width = width
        self.collapsed = collapsed
        super().__init__(*args, **kwargs)
    
    def __call__(self):
        if self.collapsed:
            actual_width = 80 if isinstance(self.width, int) else "80px"
        else:
            actual_width = self.width
        
        if isinstance(actual_width, int):
            self.attributes['style'] = f"width: {actual_width}px; {self.attributes.get('style', '')}"
        else:
            self.attributes['style'] = f"width: {actual_width}; {self.attributes.get('style', '')}"
        
        tidy_attrs = self.TidyAttributes(self.attributes)
        return html.aside(tidy_attrs, *self.children)


class Content(AdvancedComponent):
    """Content layout component"""
    
    DefaultAttributes: ClassVar[ComponentAttributes] = {
        'class_': 'flex-1 p-4 bg-gray-50 overflow-auto'
    }
    
    @overload
    def __init__(
        self,
        *children: ComponentType,
        **attributes: Unpack[ComponentAttributes]
    ): ...
    
    @overload
    def __init__(
        self,
        attributes: ComponentAttributes,
        *children: ComponentType,
        **kwargs: Unpack[ComponentAttributes]
    ): ...
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def __call__(self):
        tidy_attrs = self.TidyAttributes(self.attributes)
        return html.main(tidy_attrs, *self.children)


class Space(AdvancedComponent):
    """Space component for adding spacing between elements"""
    
    DefaultAttributes: ClassVar[ComponentAttributes] = {
        'class_': 'flex'
    }
    
    direction: _Direction
    size: _Gap
    align: _AlignItems
    wrap: _FlexWrap
    
    @overload
    def __init__(
        self,
        *children: ComponentType,
        direction: _Direction = 'row',
        size: _Gap = 'md',
        align: _AlignItems = 'start',
        wrap: _FlexWrap = 'nowrap',
        **attributes: Unpack[ComponentAttributes]
    ): ...
    
    @overload
    def __init__(
        self,
        attributes: ComponentAttributes,
        *children: ComponentType,
        direction: _Direction = 'row',
        size: _Gap = 'md',
        align: _AlignItems = 'start',
        wrap: _FlexWrap = 'nowrap',
        **kwargs: Unpack[ComponentAttributes]
    ): ...
    
    def __init__(self, *args, direction: _Direction = 'row', size: _Gap = 'md', align: _AlignItems = 'start', wrap: _FlexWrap = 'nowrap', **kwargs):
        self.direction = direction
        self.size = size
        self.align = align
        self.wrap = wrap
        super().__init__(*args, **kwargs)
    
    def __call__(self):
        classes = []
        
        # Direction mapping
        if self.direction == 'column':
            classes.append('flex-col')
        elif self.direction == 'column-reverse':
            classes.append('flex-col-reverse')
        elif self.direction == 'row-reverse':
            classes.append('flex-row-reverse')
        # 'row' is default, no need to add class
        
        # Align items mapping
        align_map = {
            'start': 'items-start',
            'end': 'items-end',
            'center': 'items-center',
            'stretch': 'items-stretch',
            'baseline': 'items-baseline'
        }
        if self.align in align_map:
            classes.append(align_map[self.align])
        
        # Flex wrap mapping
        if self.wrap == 'wrap':
            classes.append('flex-wrap')
        elif self.wrap == 'wrap-reverse':
            classes.append('flex-wrap-reverse')
        # 'nowrap' is default, no need to add class
        
        # Handle size/gap
        if isinstance(self.size, int):
            classes.append(f'gap-{self.size}')
        elif self.size in ['xs', 'sm', 'md', 'lg', 'xl', '2xl']:
            gap_map = {'xs': '1', 'sm': '2', 'md': '4', 'lg': '6', 'xl': '8', '2xl': '12'}
            classes.append(f'gap-{gap_map[self.size]}')
        
        class_names = ' '.join(filter(None, classes))
        if class_names:
            self.attributes['class_'] = f"{self.attributes.get('class_', '')} {class_names}".strip()
        
        tidy_attrs = self.TidyAttributes(self.attributes)
        return html.div(tidy_attrs, *self.children)


class Divider(AdvancedComponent):
    """Divider component for separating content"""
    
    DefaultAttributes: ClassVar[ComponentAttributes] = {
        'class_': 'border-gray-300'
    }
    
    orientation: Literal['horizontal', 'vertical']
    dashed: bool
    
    @overload
    def __init__(
        self,
        *children: ComponentType,
        orientation: Literal['horizontal', 'vertical'] = 'horizontal',
        dashed: bool = False,
        **attributes: Unpack[ComponentAttributes]
    ): ...
    
    @overload
    def __init__(
        self,
        attributes: ComponentAttributes,
        *children: ComponentType,
        orientation: Literal['horizontal', 'vertical'] = 'horizontal',
        dashed: bool = False,
        **kwargs: Unpack[ComponentAttributes]
    ): ...
    
    def __init__(self, *args, orientation: Literal['horizontal', 'vertical'] = 'horizontal', dashed: bool = False, **kwargs):
        self.orientation = orientation
        self.dashed = dashed
        super().__init__(*args, **kwargs)
    
    def __call__(self):
        if self.children:
            # Divider with text
            if self.orientation == 'horizontal':
                classes = ['flex', 'items-center', 'my-4']
                border_class = 'border-dashed' if self.dashed else 'border-solid'
                
                self.attributes['class_'] = f"{self.attributes.get('class_', '')} {' '.join(classes)}".strip()
                tidy_attrs = self.TidyAttributes(self.attributes)
                
                return html.div(
                    tidy_attrs,
                    html.div({'class': f'flex-1 border-t {border_class} border-gray-300'}),
                    html.span({'class': 'px-3 text-gray-500 text-sm'}, *self.children),
                    html.div({'class': f'flex-1 border-t {border_class} border-gray-300'})
                )
            else:
                classes = ['flex', 'items-center', 'mx-4']
                border_class = 'border-dashed' if self.dashed else 'border-solid'
                
                self.attributes['class_'] = f"{self.attributes.get('class_', '')} {' '.join(classes)}".strip()
                tidy_attrs = self.TidyAttributes(self.attributes)
                
                return html.div(
                    tidy_attrs,
                    html.div({'class': f'flex-1 border-l {border_class} border-gray-300'}),
                    html.span({'class': 'px-3 text-gray-500 text-sm'}, *self.children),
                    html.div({'class': f'flex-1 border-l {border_class} border-gray-300'})
                )
        else:
            # Simple divider line
            if self.orientation == 'horizontal':
                classes = ['w-full', 'my-4']
                if self.dashed:
                    classes.extend(['border-t', 'border-dashed'])
                else:
                    classes.extend(['border-t', 'border-solid'])
            else:
                classes = ['h-full', 'mx-4']
                if self.dashed:
                    classes.extend(['border-l', 'border-dashed'])
                else:
                    classes.extend(['border-l', 'border-solid'])
            
            class_names = ' '.join(classes)
            self.attributes['class_'] = f"{self.attributes.get('class_', '')} {class_names}".strip()
            
            tidy_attrs = self.TidyAttributes(self.attributes)
            return html.div(tidy_attrs)


class Splitter(AdvancedComponent):
    """Splitter component for resizable panels"""
    
    DefaultAttributes: ClassVar[ComponentAttributes] = {
        'class_': 'flex h-full'
    }
    
    direction: Literal['horizontal', 'vertical']
    
    @overload
    def __init__(
        self,
        *children: ComponentType,
        direction: Literal['horizontal', 'vertical'] = 'horizontal',
        **attributes: Unpack[ComponentAttributes]
    ): ...
    
    @overload
    def __init__(
        self,
        attributes: ComponentAttributes,
        *children: ComponentType,
        direction: Literal['horizontal', 'vertical'] = 'horizontal',
        **kwargs: Unpack[ComponentAttributes]
    ): ...
    
    def __init__(self, *args, direction: Literal['horizontal', 'vertical'] = 'horizontal', **kwargs):
        self.direction = direction
        super().__init__(*args, **kwargs)
    
    def __call__(self):
        if self.direction == 'vertical':
            self.attributes['class_'] = f"{self.attributes.get('class_', '')} flex-col".strip()
        
        # Add resize handles between children
        children_with_handles = []
        for i, child in enumerate(self.children):
            children_with_handles.append(child)
            if i < len(self.children) - 1:  # Not the last child
                if self.direction == 'horizontal':
                    handle = html.div({
                        'class': 'w-2 bg-gray-200 hover:bg-gray-300 cursor-col-resize flex-shrink-0',
                        'style': 'user-select: none;'
                    })
                else:
                    handle = html.div({
                        'class': 'h-2 bg-gray-200 hover:bg-gray-300 cursor-row-resize flex-shrink-0',
                        'style': 'user-select: none;'
                    })
                children_with_handles.append(handle)
        
        tidy_attrs = self.TidyAttributes(self.attributes)
        return html.div(tidy_attrs, *children_with_handles)


__all__ = [
    'Flex', 'Grid', 'Col', 'Layout', 'Header', 'Footer', 'Sider', 'Content', 
    'Space', 'Divider', 'Splitter'
]