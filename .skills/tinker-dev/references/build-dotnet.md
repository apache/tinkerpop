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

# .NET GLV Build and Test

## Requirements

- **.NET SDK 8.0+** — optional; Docker handles test execution via Maven.
- **Docker and Docker Compose** — required for running tests through Maven.
- **Maven** — preferred build orchestration.
- **Mono** — only needed for packing the `Gremlin.Net.Template` project.

## Module Structure

```
gremlin-dotnet/
├── pom.xml                 Maven parent (packaging=pom)
├── Gremlin.Net.sln         .NET solution file
├── src/                    Source code (Gremlin.Net library + template)
│   ├── .glv                Sentinel file to activate build
│   └── Gremlin.Net/        Main library
├── test/                   Test projects
│   ├── .glv                Sentinel file to activate tests
│   ├── Gremlin.Net.UnitTest/
│   ├── Gremlin.Net.IntegrationTest/
│   ├── Gremlin.Net.Benchmarks/
│   └── Gremlin.Net.Template.IntegrationTest/
├── Examples/               Example projects
└── docker-compose.yml      Test infrastructure
```

## Activation

Both `src` and `test` directories need `.glv` sentinel files:
```bash
touch gremlin-dotnet/src/.glv
touch gremlin-dotnet/test/.glv
```

Without these files, TinkerPop still builds with Maven but .NET projects are skipped.

## Maven Build Commands

Run from the repository root.

Build and test (requires Docker):
```bash
mvn clean install -pl :gremlin-dotnet,:gremlin-dotnet-source,:gremlin-dotnet-tests
```

The test execution uses Docker Compose to:
1. Start a Gremlin Server test container (`gremlin-server-test-dotnet`)
2. Run `dotnet test ./Gremlin.Net.sln -c Release` inside a .NET SDK 8.0 container
3. Run example projects to verify they work
4. Tear down containers

## Docker Test Infrastructure

The `docker-compose.yml` defines:
- `gremlin-server-test-dotnet` — Gremlin Server on ports 45940-45942 and 4588
- `gremlin-dotnet-integration-tests` — .NET SDK 8.0 container that runs `dotnet test`

The test container uses `dotnet-trx` for test result reporting.

## Test Results

Test results are written as `.trx` files inside the container. On failure, check the
Maven console output for the `dotnet test` exit code and error summary.

## Template Packaging

To pack the `Gremlin.Net.Template` NuGet package (requires Mono):
```bash
mvn clean install -Dnuget
```

## Evaluating Build Results

Use the Maven exit code to determine pass/fail:
- Exit `0` — success
- Non-zero — failure; check the `dotnet test` output in the Maven console
