### Available Environment Variables
- `THINKSERVE_HOST`
Defines the host address for the ThinkServe server. By default, it is set to `localhost`.

- `THINKSERVE_PORT`
Defines the port number for the ThinkServe server. By default, it is set to `9394`.

- `THINKSERVE_AUTH`
Defines the authentication token for connecting to the ThinkServe server. By default, it is set to `None`.

- `THINKSERVE_LOG_LEVEL`
Sets the logging level for ThinkServe. Possible values are: `VERBOSE`, `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Default is `INFO`.

- `THINKSERVE_STORAGE_DIR`
The path for storing service-related files. By default, it uses `{pwd}/.thinkserve_storage`.

- `THINKSERVE_RPC_SKIP_GENERATION` / `PYDANTIC_RPC_SKIP_GENERATION`:
By default, PydanticRPC generates .proto files and code at runtime. If you wish to skip the code-generation step (for example, in production environment), set it to "1" or "true" (case insensitive).


- `THINKSERVE_RPC_PROTO_PATH` / `PYDANTIC_RPC_PROTO_PATH`:
The path for storing the generated .proto files. By default it will be `THINKSERVE_STORAGE_DIR/protos`.


- `THINKSERVE_RPC_RESERVED_FIELDS` / `PYDANTIC_RPC_RESERVED_FIELDS`:
You can also set an environment variable to reserve a set number of fields for proto generation, for backward and forward compatibility.