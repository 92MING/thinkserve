from pathlib import Path
from typing import Literal
from functools import cache
from pydantic import BaseModel, model_validator

def _simplify_name(name: str) -> str:
    return name.lower().replace(' ', '').replace('_', '').replace('-', '').strip()

@cache
def _thinkserve_configs_field_name_mapper():
    mapper = {}
    for field_name in ThinkServeConfigs.model_fields:
        simple_name = _simplify_name(field_name)
        mapper[simple_name] = field_name
    return mapper

class ThinkServeConfigs(BaseModel):
    id: str = ''
    '''a unique identifier for the ThinkServe server. If not given, a default id will be generated.'''
    name: str = ''
    '''The name of the ThinkServe server. If not given, a default name will be used.'''
    auth: str|None = ''
    '''The authentication token for the ThinkServe server. If not given, no authentication is required.'''
    host: str = ''
    '''The host address for the ThinkServe server.'''
    port: int = -1
    '''The port number for the ThinkServe server.'''
    log_level: Literal['VERBOSE', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']|str = ''
    '''The log level for the ThinkServe server.'''
    
    storage_path: str = ''
    '''The parent dir for storing ThinkServe data, including images, protobufs, ...
    If not given, a default path will be used.'''
    image_path: str = ''
    '''The path to store images. Default to be {storage_path}/images.'''
    grpc_protobuf_path: str = ''
    '''The path to the gRPC protobuf definitions. Default to be {storage_path}/protos.'''
    
    enable_grpc: bool = False
    '''Whether to enable gRPC server.'''
    enable_http: bool = True
    '''Whether to enable HTTP server.'''
    
    @classmethod
    def TidyConfigFieldName(cls, name: str) -> str|None:
        '''Get the actual field name from a simplified name.'''
        simple_name = _simplify_name(name)
        mapper = _thinkserve_configs_field_name_mapper()
        return mapper.get(simple_name, None)
    
    @model_validator(mode='before')
    @classmethod
    def _PreValidator(cls, data):
        if isinstance(data, dict):
            mapper = _thinkserve_configs_field_name_mapper()
            new_data = {}
            for key, value in data.items():
                simple_key = _simplify_name(key)
                if simple_key in mapper:
                    new_data[mapper[simple_key]] = value
            return new_data
        return data
    
    def model_post_init(self, _) -> None:
        if not self.name:
            from ..service.comm import _get_local_ip
            self.name = f'thinkserve-{_get_local_ip()}:{self.port}'
        if self.auth == '':
            from ..common_utils.constants import THINKSERVE_AUTH
            self.auth = THINKSERVE_AUTH
        if not self.host:
            from ..common_utils.constants import THINKSERVE_HOST
            self.host = THINKSERVE_HOST
        if self.port <= 0:
            from ..common_utils.constants import THINKSERVE_PORT
            self.port = THINKSERVE_PORT
        if not self.log_level:
            from ..common_utils.constants import THINKSERVE_LOG_LEVEL
            self.log_level = THINKSERVE_LOG_LEVEL
        self.log_level = self.log_level.upper()
        
        if not self.storage_path:
            from ..common_utils.constants import STORAGE_DIR
            self.storage_path = STORAGE_DIR.as_posix()
        if not self.image_path:
            self.image_path = (Path(self.storage_path) / 'images').as_posix()
        if not self.grpc_protobuf_path:
            self.grpc_protobuf_path = (Path(self.storage_path) / 'protos').as_posix()
        
            
            
__all__ = ['ThinkServeConfigs']