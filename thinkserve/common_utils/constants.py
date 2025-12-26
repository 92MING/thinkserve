import os

from pathlib import Path

THINKSERVE_HOST = os.environ.get('THINKSERVE_HOST', 'localhost')
try:
    THINKSERVE_PORT = int(os.environ.get('THINKSERVE_PORT', '9394').strip())
except ValueError:
    THINKSERVE_PORT = 9394
THINKSERVE_AUTH = os.environ.get('THINKSERVE_AUTH', None)

THINKSERVE_LOG_LEVEL = os.environ.get('THINKSERVE_LOG_LEVEL', 'INFO').upper()
if THINKSERVE_LOG_LEVEL not in ('VERBOSE', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'):
    THINKSERVE_LOG_LEVEL = 'INFO'

def _tidy_dir(p: Path) -> Path:
    p = p.expanduser().resolve()
    if not p.exists():
        p.mkdir(parents=True, exist_ok=True)
    if not os.access(p, os.W_OK):
        raise PermissionError(f"Directory '{p}' is not writable.")
    return p

if 'THINKSERVE_STORAGE_DIR' in os.environ:
    STORAGE_DIR = Path(os.environ['THINKSERVE_STORAGE_DIR'])
else: 
    STORAGE_DIR = Path.cwd() / ".thinkserve_storage"
    
TEMP_PATH = STORAGE_DIR / "temp"

if 'THINKSERVE_RPC_PROTO_PATH' in os.environ:
    PROTO_PATH = Path(os.environ["THINKSERVE_RPC_PROTO_PATH"])
elif 'PYDANTIC_RPC_PROTO_PATH' in os.environ:
    PROTO_PATH = Path(os.environ["PYDANTIC_RPC_PROTO_PATH"])
else:
    PROTO_PATH = STORAGE_DIR / "protos"

for p in [STORAGE_DIR, TEMP_PATH, PROTO_PATH]:
    _tidy_dir(p)


__all__ = [
    "THINKSERVE_HOST",
    "THINKSERVE_PORT",
    "THINKSERVE_AUTH", 
    "THINKSERVE_LOG_LEVEL",
            
    "STORAGE_DIR",
    "TEMP_PATH",
    "PROTO_PATH",
]
