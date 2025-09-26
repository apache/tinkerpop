<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# TinkerPop GLV Examples

This directory contains ready-to-run examples for all Gremlin Language Variants (GLVs) using the latest published driver versions. These examples work out-of-the-box without requiring you to build TinkerPop from source.

## Server Configuration

**Important**: Different examples require different server configurations:

- **BasicGremlin & Connections**: Use clean server with `conf/gremlin-server.yaml`
- **ModernTraversals**: Requires server with Modern graph preloaded using `conf/gremlin-server-modern.yaml`

Start server with Docker:
```bash
# For BasicGremlin and Connections
docker run -d -p 8182:8182 tinkerpop/gremlin-server conf/gremlin-server.yaml

# For ModernTraversals  
docker run -d -p 8182:8182 tinkerpop/gremlin-server conf/gremlin-server-modern.yaml
```

## Examples Description

- **BasicGremlin**: Simple connection and basic traversals - works with clean server
- **Connections**: Various connection configuration options - works with clean server  
- **ModernTraversals**: Complex traversals using the Modern graph dataset - requires preloaded graph

## Documentation

For complete documentation and advanced usage, see the [official TinkerPop Gremlin Variants documentation](https://tinkerpop.apache.org/docs/current/reference/#gremlin-variants).

## Quick Start

### Go
```bash
cd gremlin-go
go run basic_gremlin.go
go run connections.go
go run modern_traversals.go  # Requires modern graph
```

### Java
```bash
cd gremlin-java
mvn clean install
java -cp target/run-examples-shaded.jar examples.BasicGremlin
java -cp target/run-examples-shaded.jar examples.Connections
java -cp target/run-examples-shaded.jar examples.ModernTraversals  # Requires modern graph
```

### JavaScript
```bash
cd gremlin-javascript
npm install
node basic-gremlin.js
node connections.js
node modern-traversals.js  # Requires modern graph
```

### Python
```bash
cd gremlin-python
pip install -r requirements.txt
python basic_gremlin.py
python connections.py
python modern_traversals.py  # Requires modern graph
```

### .NET
```bash
cd gremlin-dotnet
dotnet build Examples.sln
dotnet run --project BasicGremlin
dotnet run --project Connections
dotnet run --project ModernTraversals  # Requires modern graph
```

## Notes

- These examples use published driver versions (not local development code)
- For development examples using local code, see individual GLV directories
- Driver versions are pinned to the latest stable release
