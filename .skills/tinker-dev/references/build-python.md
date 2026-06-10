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

# Python GLV Build and Test

## Requirements

- **Python 3.9+** — optional for local development; Docker handles test execution via Maven.
- **Docker and Docker Compose** — required for running tests through Maven.
- **Maven** — preferred build orchestration.

## Activation

Create a `.glv` sentinel file to include Python in standard builds:
```bash
touch gremlin-python/.glv
```

Or use the Maven profile explicitly:
```bash
mvn clean install -Pglv-python -pl gremlin-python
```

## Maven Build Commands

Run from the repository root.

Run all tests:
```bash
mvn clean install -pl gremlin-python -Pglv-python
```

Run only unit tests:
```bash
mvn clean install -pl gremlin-python -Pglv-python -DpytestArgs="tests/unit"
```

Run only integration tests:
```bash
mvn clean install -pl gremlin-python -Pglv-python -DpytestArgs="tests/integration"
```

Run only feature tests:
```bash
mvn clean install -pl gremlin-python -Pglv-python -DradishArgs=" "
```

Run a specific test by name:
```bash
mvn clean install -pl gremlin-python -Pglv-python -DpytestArgs="-k test_name"
```

Run feature tests with specific tags:
```bash
mvn clean install -pl gremlin-python -Pglv-python -DradishArgs="-t @some_tag"
```

## Evaluating Build Results

**Do not** use `grep`, `tail`, or similar text-search tools on Maven console output to
determine pass/fail. Maven output can be misleading because tests validate expected errors.

Follow this hierarchy:

1. **Check the exit code.** Exit `0` = success. Non-zero = failure.
2. **Inspect XML reports** on failure. Reports are in
   `gremlin-python/target/python3/python-reports/`:
   - `TEST-native-python.xml` — unit/integration tests
   - `feature-graphbinary-result.xml` — GraphBinary feature tests
   - `feature-graphbinary-params-result.xml` — GraphBinary parameterized
   - `feature-graphbinary-bulked-result.xml` — GraphBinary bulked
   - `feature-graphson-result.xml` — GraphSON feature tests
   - `feature-graphson-params-result.xml` — GraphSON parameterized

Feature test XML reports will not be present if unit/integration tests fail.

**Do not** investigate a build failure beyond reporting what failed unless asked.

## Manual Test Execution

If you must run tests outside Maven, work from `gremlin-python/src/main/python/`:

### Environment setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install .[test,kerberos]
```

### Gremlin Server management

From `gremlin-python/`:
```bash
# Start/restart server
docker compose down gremlin-server-test-python
docker compose up -d gremlin-server-test-python

# Check status (wait for healthy)
docker compose ps gremlin-server-test-python
```

### Running tests manually
- Unit tests: `pytest tests/unit/`
- Integration tests: `pytest tests/integration/`
- Feature tests:
  ```bash
  radish -f dots -e -t -b ./tests/radish \
    ../../../../gremlin-test/src/main/resources/org/apache/tinkerpop/gremlin/test/features \
    --user-data='serializer=application/vnd.gremlin-v3.0+json'
  ```
