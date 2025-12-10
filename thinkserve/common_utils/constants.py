import os

from pathlib import Path

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
    "STORAGE_DIR",
    "TEMP_PATH",
    "PROTO_PATH",
]
