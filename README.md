### Available Environment Variables

`THINKSERVE_STORAGE_DIR`
The path for storing service-related files. By default, it uses `{pwd}/.thinkserve_storage`.

`THINKSERVE_RPC_SKIP_GENERATION` / `PYDANTIC_RPC_SKIP_GENERATION`:

By default, PydanticRPC generates .proto files and code at runtime. If you wish to skip the code-generation step (for example, in production environment), set it to "1" or "true" (case insensitive).


`THINKSERVE_RPC_PROTO_PATH` / `PYDANTIC_RPC_PROTO_PATH`:
The path for storing the generated .proto files. By default it will be `THINKSERVE_STORAGE_DIR/protos`.


`THINKSERVE_RPC_RESERVED_FIELDS` / `PYDANTIC_RPC_RESERVED_FIELDS`:

You can also set an environment variable to reserve a set number of fields for proto generation, for backward and forward compatibility.
