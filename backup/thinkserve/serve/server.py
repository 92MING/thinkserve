from pathlib import Path
from dataclasses import dataclass
from typing import Literal

from .configs import ThinkServeConfigs
from ..service.comm import EventSocketServer, EventSocketClient, EventCommunicationBase
from ..service.service import _event, _invoke_event

from ..common_utils.debug_utils import Logger, get_logger, LogMessage

@dataclass
class ThinkServePeer:
    type: Literal['master', 'slave', 'partner', 'friend']
    '''
    Permission:
    ```
    |---------|-------------|----------------|------------------|
    | Type(to)| access info | access services| control services |
    |---------|-------------|----------------|------------------|
    | master  |     no      |      no        |       no         |
    | slave   |     yes     |      yes       |       yes        |
    | partner |     yes     |      yes       |       yes        |   # master-master
    | friend  |     yes     |      yes       |       no         |
    ```
    '''
    name: str
    client: EventCommunicationBase

    @property
    def id(self)->str:
        if not (this_id:=getattr(self, '_id', None)):
            this_id = next(iter(self.client._clients.keys()))
            self._id = this_id
        return this_id
    
class ThinkServe:
    
    _server: EventSocketServer
    '''The event socket server for ThinkServe to manage services.'''
    _config: ThinkServeConfigs
    '''The configurations of ThinkServe server.'''
    _started: bool = False
    '''Whether the server is started.'''
    _peers: dict[str, ThinkServePeer]
    '''The connected peers. Key is peer id(an internal unique string).'''
    
    def __init__(
        self,
        config: str|Path|ThinkServeConfigs|None=None,
    ):
        if not config:
            self._config = ThinkServeConfigs()
        elif isinstance(config, ThinkServeConfigs):
            self._config = config
        elif isinstance(config, (str, Path)):
            if isinstance(config, str) and config.strip().startswith('{'):
                self._config = ThinkServeConfigs.model_validate_json(config)
            else:   # Path
                self._config = ThinkServeConfigs.model_validate_json(Path(config).read_text())
        else:
            raise TypeError(f'Invalid type for config: {type(config)}')
        self._server = EventSocketServer(
            host=self._config.host,
            port=self._config.port,
            auth=self._config.auth,
            name=self._config.name,
        )
    
    # region properties
    @property
    def name(self)->str:
        return self._config.name
        
    @property
    def logger(self)->Logger:
        if not (logger:=getattr(self, '_logger', None)):
            logger = self._logger = get_logger(self.name)
            if __name__ != '__main__':
                ...
        return logger
    # endregion
    
    # region events    
    @_event
    def _service_log(self, log_msg: LogMessage):
        ...
    # endregion
    
        
    def start(self):
        if not self._started:
            self._started = True
            self._server.start()
        
    def stop(self):
        if self._started:
            self._started = False
            self._server.stop()
            
    def __del__(self):
        self.stop()
        