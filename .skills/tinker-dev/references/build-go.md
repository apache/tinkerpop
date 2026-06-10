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

# Go GLV Build and Test

## Requirements

- **Go 1.25+** — optional for local development; Docker handles test execution via Maven.
- **Docker and Docker Compose** — required for running tests through Maven.
- **Maven** — preferred build orchestration.

## Module Structure

```
gremlin-go/
├── pom.xml                 Maven module (no Java source; Maven wraps Go)
├── go.mod                  Go module definition
├── go.sum                  Go dependency checksums
├── run.sh                  Convenience script for Docker-based testing
├── driver/                 Go driver source and tests
│   ├── cucumber/           BDD feature test support
│   ├── resources/          Test resources
│   └── *.go / *_test.go    Source and test files
├── examples/               Example programs
└── docker-compose.yml      Test infrastructure
```

## Activation

Create a `.glv` sentinel file to include Go in standard builds:
```bash
touch gremlin-go/.glv
```

Without this file, TinkerPop still builds with Maven but Go is skipped.

## Maven Build Commands

Run from the repository root.

Build and test (requires Docker):
```bash
mvn clean install -Pglv-go -pl :gremlin-go
```

The Maven build uses Docker Compose to:
1. Start a Gremlin Server test container
2. Run `go test -v -json ./... -race -covermode=atomic` inside a Go container
3. Run example programs
4. Tear down containers and prune dangling images

## Convenience Script

From `gremlin-go/`:
```bash
./run.sh                    # Uses current project version
./run.sh 4.0.0-SNAPSHOT     # Specify server version
./run.sh 3.8.0              # Use a released version
```

The default requires a SNAPSHOT server image built with:
```bash
mvn clean install -pl :gremlin-server -DskipTests -DskipIntegrationTests=true -am
mvn install -Pdocker-images -pl :gremlin-server
```

## Docker Test Infrastructure

The `docker-compose.yml` defines:
- `gremlin-server-test` — Gremlin Server on ports 45940-45942 and 4588
- `gremlin-go-integration-tests` — Go 1.25 container that runs tests with race
  detection, coverage, and `gotestfmt` for formatted output

Environment variables in the test container:
- `CUCUMBER_FEATURE_FOLDER` — path to Gherkin feature files
- `GREMLIN_SERVER_URL` — server endpoint for integration tests
- `RUN_INTEGRATION_TESTS=true`
- `RUN_BASIC_AUTH_INTEGRATION_TESTS=true`

## Local Development

After installing Go locally:
```bash
cd gremlin-go
go build ./...
go test ./driver/ -run TestName
```

For integration tests, a running Gremlin Server is required.

## Evaluating Build Results

Use the Maven exit code to determine pass/fail:
- Exit `0` — success
- Non-zero — failure; check the `go test` output in the Maven console
