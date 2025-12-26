import os
import logging

from typing import TYPE_CHECKING
from pydantic import BaseModel, Field

if not TYPE_CHECKING:
    logging.addLevelName(5, "VERBOSE")
    logging.addLevelName(100, "SUCCESS")
logging.VERBOSE = 5     # type: ignore
logging.SUCCESS = 100   # type: ignore

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

class LogMessage(BaseModel):
    name: str
    '''logger name'''
    level: str
    '''log level, e.g., INFO, DEBUG, VERBOSE, SUCCESS, WARNING, ERROR.
    This field is not restricted with Literal, to allow custom log levels in future.'''
    message: str
    '''the log message'''
    pid: int = Field(default_factory=os.getpid)
    '''the process ID'''


__all__ = ['Logger', 'get_logger', 'LogMessage']