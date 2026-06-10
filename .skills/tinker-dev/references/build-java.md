<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Java / Core Module Builds

## Full Build (skip tests)

When modules have unresolved dependency ordering issues (e.g., shaded jars not yet installed, branches changed, etc.), 
build the entire project first:

```bash
mvn clean install -DskipTests
```

## Building and Test Specific Modules

Build and test a single module:
```bash
mvn clean install -pl <module-name>
```

Examples:
```bash
mvn clean install -pl :tinkergraph-gremlin
mvn clean install -pl :gremlin-core
mvn clean install -pl :gremlin-server
```

Build and test a module and its dependencies:
```bash
mvn clean install -pl :gremlin-server -am
```

## Gremlin Server

Build gremlin-server (commonly needed before GLV testing):
```bash
mvn clean install -pl :gremlin-server -am -DskipTests
```

Run gremlin-server integration tests:
```bash
mvn clean install -pl :gremlin-server -DskipIntegrationTests=false
```

## Integration Tests

Build and test with all integration tests across the project:
```bash
mvn clean install -DskipIntegrationTests=false
```
