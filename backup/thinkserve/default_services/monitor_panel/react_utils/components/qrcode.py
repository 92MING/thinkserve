import io
import base64
import qrcode
import logging

from pathlib import Path
from reactpy import html
from PIL import Image, ImageDraw
from functools import lru_cache
from typing import Literal, ClassVar, overload, Unpack, Any

from qrcode.image.styledpil import StyledPilImage
from qrcode.image.styles.moduledrawers import (
    SquareModuleDrawer, CircleModuleDrawer, RoundedModuleDrawer,
    VerticalBarsDrawer, HorizontalBarsDrawer, GappedSquareModuleDrawer
)
from qrcode.image.styles.colormasks import SquareGradiantColorMask, RadialGradiantColorMask, SolidFillColorMask

from .base import AdvancedComponent, ComponentAttributes, ComponentType
from ..state import State
from .._internal.file_utils import load_file_data

type _QRCodeStyle = Literal['square', 'circle', 'rounded', 'vertical_bars', 'horizontal_bars', 'gapped_square']
type _ColorMaskType = Literal['solid', 'square_gradient', 'radial_gradient']
type _ErrorCorrection = Literal['L', 'M', 'Q', 'H']

def _hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    """Convert hex color to RGB tuple"""
    hex_color = hex_color.lstrip('#')
    if len(hex_color) != 6:
        # Default to black if invalid hex
        hex_color = "000000"
    return (int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16))

_logger = logging.getLogger(__name__)

@lru_cache
def _gen_qrcode(
    data: str,
    err_correction_level: int,
    box_size: int,
    border: int,
    qr_style: str,
    color_mask_type: _ColorMaskType,
    back_color: str,
    fill_color: str,
    gradient_start: str,
    gradient_end: str
):
    qr = qrcode.QRCode(
        version=1,
        error_correction=err_correction_level,
        box_size=box_size,
        border=border,
    )
    qr.add_data(data)
    qr.make(fit=True)
    
    # Create image with styling
    module_drawer = _style_map.get(qr_style, _default_drawer)
    color_mask = _get_color_mask(
        color_mask_type=color_mask_type,
        back_color=back_color,
        fill_color=fill_color,
        gradient_start=gradient_start,
        gradient_end=gradient_end
    )
    img = qr.make_image(
        image_factory=StyledPilImage,
        module_drawer=module_drawer,
        color_mask=color_mask
    )
    if img.mode != 'RGB':
        img = img.convert('RGB')
    
    return img

# region constants
_error_correction_levels = {
    'L': qrcode.ERROR_CORRECT_L,
    'M': qrcode.ERROR_CORRECT_M,
    'Q': qrcode.ERROR_CORRECT_Q,
    'H': qrcode.ERROR_CORRECT_H
}

_default_drawer = SquareModuleDrawer()
_style_map = {
    'square': _default_drawer,
    'circle': CircleModuleDrawer(),
    'rounded': RoundedModuleDrawer(),
    'vertical_bars': VerticalBarsDrawer(),
    'horizontal_bars': HorizontalBarsDrawer(),
    'gapped_square': GappedSquareModuleDrawer()
}

@lru_cache
def _get_square_gradient_color_mask(back_color: str):
    return SquareGradiantColorMask(
        back_color=_hex_to_rgb(back_color)
    )
@lru_cache
def _get_radial_gradient_color_mask(
    back_color: str,
    center_color: str,
    edge_color: str
) -> RadialGradiantColorMask:
    return RadialGradiantColorMask(
        back_color=_hex_to_rgb(back_color),
        center_color=_hex_to_rgb(center_color),
        edge_color=_hex_to_rgb(edge_color)
    )
@lru_cache
def _get_solid_fill_color_mask(
    back_color: str,
    front_color: str
) -> SolidFillColorMask:
    return SolidFillColorMask(
        back_color=_hex_to_rgb(back_color),
        front_color=_hex_to_rgb(front_color)
    )

def _get_color_mask(
    color_mask_type: _ColorMaskType,
    back_color: str,
    fill_color: str,
    gradient_start: str,
    gradient_end: str,
) -> Any:
    """Get the appropriate color mask based on type"""
    if color_mask_type == "square_gradient":
        return _get_square_gradient_color_mask(back_color)
    elif color_mask_type == "radial_gradient":
        return _get_radial_gradient_color_mask(
            back_color=back_color,
            center_color=gradient_start,
            edge_color=gradient_end
        )
    else:  # solid
        return _get_solid_fill_color_mask(
            back_color=back_color,
            front_color=fill_color
        )
# endregion

class QRCode(AdvancedComponent):
    """QRCode component with rich styling options"""
    
    DefaultAttributes: ClassVar[ComponentAttributes] = {
        'class_': 'inline-block'
    }
    
    # States
    data = State[str]("")
    """QR code data content"""
    qr_style = State[_QRCodeStyle]("square")
    """QR code module style"""
    fill_color = State[str]("#000000")
    """Fill color for QR code modules"""
    back_color = State[str]("#FFFFFF")
    """Background color for QR code"""
    gradient_start = State[str]("#000000")
    """Gradient start color (for gradient color masks)"""
    gradient_end = State[str]("#666666")
    """Gradient end color (for gradient color masks)"""
    color_mask_type = State[_ColorMaskType]("solid")
    """Color mask type for styling"""
    border = State[int](4)
    """Border size around QR code"""
    box_size = State[int](10)
    """Size of each QR code module in pixels"""
    error_correction = State[_ErrorCorrection]("M")
    """Error correction level"""
    center_image_data = State[str|bytes|Image.Image|Path]("")
    """Base64 encoded center image data"""
    center_image_size_ratio = State[float](0.3)
    """Size ratio of center image relative to QR code (0.1 to 0.4)"""
    
    @overload
    def __init__(
        self, 
        attribute: ComponentAttributes,
        *children: ComponentType,
        data: str|bytes = "",
        qr_style: _QRCodeStyle = "square",
        fill_color: str = "#000000",
        back_color: str = "#FFFFFF",
        gradient_start: str = "#000000", 
        gradient_end: str = "#666666",
        color_mask_type: _ColorMaskType = "solid",
        border: int = 4,
        box_size: int = 10,
        error_correction: _ErrorCorrection = "M",
        center_image: bytes | str = "",
        center_image_size_ratio: float = 0.3,
        width: int | str = "auto",
        height: int | str = "auto",
        **attributes: Unpack[ComponentAttributes]
    ): ...
    
    @overload
    def __init__(
        self, 
        *children: ComponentType,
        data: str|bytes = "",
        qr_style: _QRCodeStyle = "square",
        fill_color: str = "#000000",
        back_color: str = "#FFFFFF",
        gradient_start: str = "#000000", 
        gradient_end: str = "#666666",
        color_mask_type: _ColorMaskType = "solid",
        border: int = 4,
        box_size: int = 10,
        error_correction: _ErrorCorrection = "M",
        center_image: bytes | str = "",
        center_image_size_ratio: float = 0.3,
        width: int | str = "auto",
        height: int | str = "auto",
        **attributes: Unpack[ComponentAttributes]
    ):  ...
    
    def __init__(
        self, 
        *children,
        data: str|bytes = "",
        qr_style: _QRCodeStyle = "square",
        fill_color: str = "#000000",
        back_color: str = "#FFFFFF",
        gradient_start: str = "#000000", 
        gradient_end: str = "#666666",
        color_mask_type: _ColorMaskType = "solid",
        border: int = 4,
        box_size: int = 10,
        error_correction: _ErrorCorrection = "M",
        center_image: bytes | str = "",
        center_image_size_ratio: float = 0.3,
        width: int | str = "auto",
        height: int | str = "auto",
        **attributes: Unpack[ComponentAttributes]
    ):
        super().__init__(*children, **attributes)
        
        if not self.IsRerender():
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            self.data = data    # type: ignore
            self.qr_style = qr_style
            self.fill_color = fill_color
            self.back_color = back_color
            self.gradient_start = gradient_start
            self.gradient_end = gradient_end
            self.color_mask_type = color_mask_type
            self.border = border
            self.box_size = box_size
            self.error_correction = error_correction
            self.center_image_size_ratio = max(0.1, min(0.4, center_image_size_ratio))
            self.center_image_data = center_image
                
        self._width = width
        self._height = height
        
    def _get_color_mask(self) -> Any:
        """Get the appropriate color mask based on type"""
        if self.color_mask_type == "square_gradient":
            return SquareGradiantColorMask(
                back_color=_hex_to_rgb(self.back_color)
            )
        elif self.color_mask_type == "radial_gradient":
            return RadialGradiantColorMask(
                back_color=_hex_to_rgb(self.back_color),
                center_color=_hex_to_rgb(self.gradient_start),
                edge_color=_hex_to_rgb(self.gradient_end)
            )
        else:  # solid
            return SolidFillColorMask(
                back_color=_hex_to_rgb(self.back_color),
                front_color=_hex_to_rgb(self.fill_color)
            )
    
    def _get_error_correction_level(self) -> int:
        """Convert error correction string to qrcode constant"""
        return _error_correction_levels.get(self.error_correction, qrcode.ERROR_CORRECT_M)
    
    def _add_center_image(self, qr_img: Any) -> Any:
        """Add center image to QR code if provided"""
        if not self.center_image_data:
            return qr_img
            
        try:
            # Convert QR image to PIL Image if needed
            if hasattr(qr_img, 'convert'):
                pil_img = qr_img
            else:
                # If it's a StyledPilImage, get the underlying PIL image
                pil_img = qr_img.get_image()
            
            # Decode base64 image
            image_data = self.center_image_data
            if not isinstance(image_data, Image.Image):
                center_img = Image.open(load_file_data(image_data))
            else:
                center_img = image_data
                
            # Calculate center image size
            qr_width, qr_height = pil_img.size
            center_size = int(min(qr_width, qr_height) * self.center_image_size_ratio)
            
            # Resize center image
            center_img = center_img.resize((center_size, center_size), Image.Resampling.LANCZOS)
            
            # Convert to RGBA if not already
            if center_img.mode != 'RGBA':
                center_img = center_img.convert('RGBA')
            
            # Convert QR image to RGBA for compositing
            if pil_img.mode != 'RGBA':
                pil_img = pil_img.convert('RGBA')
            
            # Create a white background circle for the center image
            background = Image.new('RGBA', (center_size + 20, center_size + 20), (255, 255, 255, 255))
            mask = Image.new('L', (center_size + 20, center_size + 20), 0)
            draw = ImageDraw.Draw(mask)
            draw.ellipse((0, 0, center_size + 20, center_size + 20), fill=255)
            
            # Apply mask to background
            background.putalpha(mask)
            
            # Paste background circle
            bg_pos = ((qr_width - center_size - 20) // 2, (qr_height - center_size - 20) // 2)
            pil_img.paste(background, bg_pos, background)
            
            # Paste center image
            center_pos = ((qr_width - center_size) // 2, (qr_height - center_size) // 2)
            pil_img.paste(center_img, center_pos, center_img)
            
            return pil_img
            
        except Exception:
            # If center image processing fails, return original QR code
            return qr_img
    
    def _generate_qr_image(self) -> str:
        """Generate QR code image and return as base64 string"""
        if not self.data:
            return ""

        try:
            # Handle string data only (bytes should be converted to base64 before setting)
            qr_data = str(self.data)
            img = _gen_qrcode(
                data=qr_data,
                err_correction_level=self._get_error_correction_level(),
                box_size=self.box_size,
                border=self.border,
                qr_style=self.qr_style,
                color_mask_type=self.color_mask_type,
                back_color=self.back_color,
                fill_color=self.fill_color,
                gradient_start=self.gradient_start,
                gradient_end=self.gradient_end
            )
            # Add center image if provided
            img = self._add_center_image(img)
            # Convert to base64
            buffer = io.BytesIO()
            img.save(buffer, format='PNG')
            img_str = base64.b64encode(buffer.getvalue()).decode()
            
            return img_str
            
        except Exception:
            # Return empty string if generation fails
            return ""
    
    def clear_center_image(self) -> None:
        """Remove center image"""
        self.center_image_data = ""
    
    def __call__(self):
        # Generate QR code image
        _logger.debug(f"Generating QR code with data: {self.data}...")
        qr_image_b64 = self._generate_qr_image()
        
        if not qr_image_b64:
            # Show placeholder if no data or generation failed
            self.attributes['class_'] = f"{self.attributes.get('class_', '')} flex items-center justify-center bg-gray-100 text-gray-500".strip()
            
            placeholder_style = {
                'width': f'{self._width}px' if isinstance(self._width, int) else str(self._width),
                'height': f'{self._height}px' if isinstance(self._height, int) else str(self._height),
                'min-width': '100px',
                'min-height': '100px'
            }
            
            if 'style' in self.attributes:
                if isinstance(self.attributes['style'], dict):
                    self.attributes['style'].update(placeholder_style)
                else:
                    # Convert string style to dict and merge
                    style_str = self.attributes['style']
                    style_parts = [f"{k}: {v}" for k, v in placeholder_style.items()]
                    self.attributes['style'] = f"{style_str}; {'; '.join(style_parts)}"
            else:
                self.attributes['style'] = placeholder_style
            
            tidy_attrs = self.TidyAttributes(self.attributes)
            return html.div(tidy_attrs, "No QR Data", *self.children)
        
        # Create image element
        img_attrs = {
            'src': f'data:image/png;base64,{qr_image_b64}',
            'alt': 'QR Code'
        }
        
        # Handle size attributes
        if isinstance(self._width, int):
            img_attrs['width'] = str(self._width)
        elif self._width != "auto":
            img_attrs['style'] = f"width: {self._width};"
            
        if isinstance(self._height, int):
            img_attrs['height'] = str(self._height)
        elif self._height != "auto":
            current_style = img_attrs.get('style', '')
            img_attrs['style'] = f"{current_style} height: {self._height};"
        
        # Merge with component attributes
        tidy_attrs = self.TidyAttributes(self.attributes)
        
        return html.div(
            tidy_attrs,
            html.img(img_attrs),
            *self.children
        )


__all__ = ['QRCode']