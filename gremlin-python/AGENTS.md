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
### gremlin-python AGENTS.md

This document provides stack-specific guidance for developers and AI agents working on the `gremlin-python` module. It 
supplements the root `AGENTS.md` file.

---

### 1. Build and Test Requirements

To build and run tests for `gremlin-python`, ensure the following:

*   **Python Version**: Python 3.9 or higher must be installed.
*   **Virtual Environment**: While a virtual environment is recommended for local development, the preferred way to 
    run tests is via Maven and Docker, which handles environment isolation automatically.
*   **Docker**: Docker and Docker Compose must be installed and running.
*   **Maven**: Maven must be installed.

### 2. Preferred Build Orchestration

The preferred way to run tests is via Maven from the repository root. This ensures an OS-agnostic execution environment
and handles all dependencies, including starting the Gremlin Server.

*   **Run all tests**:
    ```bash
    mvn clean install -pl gremlin-python -Pglv-python
    ```

*   **Run only Unit Tests**:
    ```bash
    mvn clean install -pl gremlin-python -Pglv-python -DpytestArgs="tests/unit"
    ```

*   **Run only Integration Tests**:
    ```bash
    mvn clean install -pl gremlin-python -Pglv-python -DpytestArgs="tests/integration"
    ```

*   **Run only Feature Tests**:
    ```bash
    mvn clean install -pl gremlin-python -Pglv-python -DradishArgs=" " 
    ```

### 3. Targeted Test Execution

You can pass arguments to `pytest` or `radish` via Maven properties to run specific tests.

*   **Run a specific Unit/Integration test**:
    ```bash
    mvn clean install -pl gremlin-python -Pglv-python -DpytestArgs="-k test_name"
    ```

*   **Run Feature tests with specific tags**:
    ```bash
    mvn clean install -pl gremlin-python -Pglv-python -DradishArgs="-t @some_tag"
    ```
    
### 4. Evaluating Build Results

**DO NOT** use `grep`, `tail` or similar text-search tools on the Maven console output (or `.output.txt`) to determine 
if the tests passed or failed. The Maven output can be misleading due to the tests validating expected errors. 

To accurately evaluate the results, follow this strict hierarchy:

1.  **Check the Command Exit Code**: A successful build **must** exit with code `0`. Any non-zero exit code is a failure.
2.  **Verify via XML Reports**: If the build fails (non-zero exit) you **must** inspect the JUnit XML reports in `gremlin-python/target/python3/python-reports/`:
    *   `TEST-native-python.xml` (Unit/Integration tests)
    *   `feature-graphbinary-result.xml` (Feature tests - GraphBinary)
    *   `feature-graphbinary-params-result.xml` (Feature tests - GraphBinary parameterized)
    *   `feature-graphbinary-bulked-result.xml` (Feature tests - GraphBinary bulked)
    *   `feature-graphson-result.xml` (Feature tests - GraphSON)
    *   `feature-graphson-params-result.xml` (Feature tests - GraphSON parameterized)

Feature test XML reports will not be present if the unit/integration tests fail.

**DO NOT** attempt to investigate a build failure beyond reporting what failed unless asked.

### 5. Manual Test Execution (Advanced)

If you must run tests manually outside Maven, follow these steps from the `gremlin-python/src/main/python/` directory.

#### 5.1 Environment Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install .[test,kerberos]
```

#### 5.2 Gremlin Server Management

Manual integration and feature tests require a running Gremlin Server. Run these from `gremlin-python/`:
```bash
# Start/Restart server
docker compose down gremlin-server-test-python
docker compose up -d gremlin-server-test-python

# Check status (wait for healthy)
docker compose ps gremlin-server-test-python
```

#### 5.3 Running Tests Manually
*   **Unit Tests**: `pytest tests/unit/`
*   **Integration Tests**: `pytest tests/integration/`
*   **Feature Tests**: 
    ```bash
    radish -f dots -e -t -b ./tests/radish ../../../../gremlin-test/src/main/resources/org/apache/tinkerpop/gremlin/test/features --user-data='serializer=application/vnd.gremlin-v3.0+json'
    ```