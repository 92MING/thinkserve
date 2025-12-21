import logging
from typing import TYPE_CHECKING

if not TYPE_CHECKING:
    logging.addLevelName(5, "VERBOSE")
    logging.VERBOSE = 5
    logging.addLevelName(100, "SUCCESS")
    logging.SUCCESS = 100

from logging import Logger as _Logger

class Logger(_Logger):
    def verbose(self, msg, *args, **kwargs):
        if self.isEnabledFor(5):
            self._log(5, msg, args, **kwargs)
            
    def success(self, msg, *args, **kwargs):
        if self.isEnabledFor(100):
            self._log(100, msg, args, **kwargs)

logging.setLoggerClass(Logger)

def get_logger(name: str) -> Logger:
    logger = logging.getLogger(name)
    if not isinstance(logger, Logger):
        logger.__class__ = Logger
    return logger   # type: ignore

            
__all__ = ['Logger', 'get_logger']