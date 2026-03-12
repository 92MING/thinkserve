from functools import lru_cache
from reactpy import html
from fastapi import FastAPI

# common meta
DefaultViewport = html.meta(dict(name="viewport", content="width=device-width, initial-scale=1.0"))
DefaultCharset = html.meta(dict(charset="UTF-8"))
DefaultDescription = html.meta(dict(name="description", content=""))
DefaultKeywords = html.meta(dict(name="keywords", content=""))
DefaultAuthor = html.meta(dict(name="author", content=""))
DefaultRobots = html.meta(dict(name="robots", content="index, follow"))
HttpEquivXUACompatible = html.meta(dict(http_equiv="X-UA-Compatible", content="IE=edge"))
HttpEquivCacheControl = html.meta(dict(http_equiv="Cache-Control", content="no-cache"))

# Open Graph
OpenGraphTitle = html.meta(dict(property="og:title", content=""))
OpenGraphDescription = html.meta(dict(property="og:description", content=""))
OpenGraphType = html.meta(dict(property="og:type", content="website"))
OpenGraphUrl = html.meta(dict(property="og:url", content=""))
OpenGraphImage = html.meta(dict(property="og:image", content=""))

# Twitter Card
TwitterCard = html.meta(dict(name="twitter:card", content="summary_large_image"))
TwitterTitle = html.meta(dict(name="twitter:title", content=""))
TwitterDescription = html.meta(dict(name="twitter:description", content=""))
TwitterImage = html.meta(dict(name="twitter:image", content=""))

# common fonts
GoogleFonts = html.link(dict(
    rel="preconnect",
    href="https://fonts.googleapis.com"
))
GoogleFontsApi = html.link(dict(
    rel="preconnect", 
    href="https://fonts.gstatic.com",
    crossorigin=""
))

# CSS related
NormalizeCSS = html.link(dict(
    rel="stylesheet",
    href="https://cdnjs.cloudflare.com/ajax/libs/normalize/8.0.1/normalize.min.css"
))
TailwindCSS = html.script(dict(src="https://cdn.tailwindcss.com"), "")
BootstrapCSS = html.link(dict(
    rel="stylesheet",
    href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css"
))
FontAwesome = html.link(dict(
    rel="stylesheet", 
    href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css"
))

# Security related
ContentSecurityPolicy = html.meta(dict(http_equiv="Content-Security-Policy", content="default-src 'self'"))
XFrameOptions = html.meta(dict(http_equiv="X-Frame-Options", content="DENY"))
XContentTypeOptions = html.meta(dict(http_equiv="X-Content-Type-Options", content="nosniff"))
ReferrerPolicy = html.meta(dict(name="referrer", content="strict-origin-when-cross-origin"))

# Mobile and PWA related
AppleMobileWebAppCapable = html.meta(dict(name="apple-mobile-web-app-capable", content="yes"))
AppleMobileWebAppStatusBarStyle = html.meta(dict(name="apple-mobile-web-app-status-bar-style", content="black-translucent"))
AppleMobileWebAppTitle = html.meta(dict(name="apple-mobile-web-app-title", content=""))
MobileWebAppCapable = html.meta(dict(name="mobile-web-app-capable", content="yes"))
ApplicationName = html.meta(dict(name="application-name", content=""))
ThemeColor = html.meta(dict(name="theme-color", content="#000000"))
MSApplicationTileColor = html.meta(dict(name="msapplication-TileColor", content="#000000"))
MSApplicationConfig = html.meta(dict(name="msapplication-config", content="/browserconfig.xml"))

# Performance and resource hints
DNSPrefetch = lambda href: html.link(dict(rel="dns-prefetch", href=href))
Preconnect = lambda href: html.link(dict(rel="preconnect", href=href))
Prefetch = lambda href: html.link(dict(rel="prefetch", href=href))
Preload = lambda href, as_type: html.link(dict(rel="preload", href=href, **{"as": as_type}))

# JSON-LD structured data
JsonLD = lambda data: html.script(dict(type="application/ld+json"), data)

# Canonical URL
Canonical = lambda url: html.link(dict(rel="canonical", href=url))

# Language and alternate versions
HrefLang = lambda lang, url: html.link(dict(rel="alternate", hreflang=lang, href=url))

# RSS/Atom feeds
RSSFeed = lambda href, title="RSS Feed": html.link(dict(rel="alternate", type="application/rss+xml", href=href, title=title))
AtomFeed = lambda href, title="Atom Feed": html.link(dict(rel="alternate", type="application/atom+xml", href=href, title=title))

# Common JS libraries
jQueryCDN = html.script(dict(src="https://code.jquery.com/jquery-3.7.1.min.js"), "")
BootstrapJS = html.script(dict(src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"), "")


@lru_cache
def icon_head(href: str="/_reactpy/assets/reactpy-logo.ico"):
    return html.link(
        {
            "rel": "icon",
            "href": href,
            "type": "image/x-icon",
        }
    )

def default_head(app: FastAPI|None=None, icon: str|None=None):
    heads = [
        DefaultCharset,
        DefaultViewport,
        TailwindCSS,
        icon_head(icon) if icon else icon_head(),
    ]
    title = getattr(app, "title", "") if app else ""
    if title:
        heads.insert(0, html.title(title))
    return tuple(heads)


__all__ = [
    "DefaultViewport", "DefaultCharset", "DefaultDescription", "DefaultKeywords",
    "DefaultAuthor", "DefaultRobots", "HttpEquivXUACompatible", "HttpEquivCacheControl",
    
    "OpenGraphTitle", "OpenGraphDescription", "OpenGraphType", "OpenGraphUrl", "OpenGraphImage",
    
    "TwitterCard", "TwitterTitle", "TwitterDescription", "TwitterImage",
    
    "GoogleFonts", "GoogleFontsApi", 
    
    "NormalizeCSS", "TailwindCSS", "BootstrapCSS", "FontAwesome",  
    
    "ContentSecurityPolicy", "XFrameOptions", "XContentTypeOptions", "ReferrerPolicy",
    
    "AppleMobileWebAppCapable", "AppleMobileWebAppStatusBarStyle", "AppleMobileWebAppTitle",
    "MobileWebAppCapable", "ApplicationName", "ThemeColor", "MSApplicationTileColor", "MSApplicationConfig",
    
    "DNSPrefetch", "Preconnect", "Prefetch", "Preload",
    
    "JsonLD", "Canonical", "HrefLang",
    
    "RSSFeed", "AtomFeed",
    
    "jQueryCDN", "BootstrapJS",
    
    "icon_head", "default_head"
]

