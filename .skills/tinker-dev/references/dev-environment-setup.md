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

# Development Environment Setup

This guide walks through setting up a TinkerPop development environment from a fresh clone.
The canonical reference is `docs/src/dev/developer/development-environment.asciidoc`.

## Prerequisites

### Minimum (Java-only build)

- **Java 11 or 17** — OpenJDK recommended. Use [SDKMAN!](https://sdkman.io/) to manage versions.
  Note: some deep reflection requires `--add-opens` JVM options, which are already configured
  in the pom files.
- **Maven 3.5.3+** — also manageable via SDKMAN.

With just Java and Maven you can build JVM modules and get a clean `mvn clean install`, but
GLV builds and many integration tests will be skipped.

### Full environment (all GLVs and integration tests)

- **Docker and Docker Compose** — required for all GLV test execution. Docker Desktop includes
  both. GLV tests run inside Docker containers, so local language runtimes are optional for
  test execution via Maven.
- **Python 3.10+** — optional for local development; Docker handles test execution.
- **Node.js 22+ / npm 10+** — optional; Maven downloads a local copy via `frontend-maven-plugin`.
  Consider [nvm](https://github.com/nvm-sh/nvm) for version management.
- **.NET SDK 8.0+** — optional; Docker handles test execution.
- **Go 1.25+** — optional; Docker handles test execution.

## Environment Verification

Run the included check script to verify your setup:

```bash
bash .skills/tinkerpop-dev/scripts/check-env.sh
```

This checks for Java, Maven, Docker, and optionally Python, Node.js, .NET, and Go.

## First Build

After cloning, verify the basic build works:

```bash
mvn clean install -DskipTests
```

This builds all JVM modules. GLV modules will be skipped unless activated (see below).

## Activating GLV Builds

Each Gremlin Language Variant uses a `.glv` sentinel file to signal Maven that the GLV
should be built as part of a standard `mvn clean install`. The file can be empty.

### Python
```bash
touch gremlin-python/.glv
```

### JavaScript / TypeScript
No `.glv` file needed — JS modules build by default via the `frontend-maven-plugin`.
Tests require Docker and are skipped with `-DskipTests`.

### .NET
Both `src` and `test` directories need the sentinel:
```bash
touch gremlin-dotnet/src/.glv
touch gremlin-dotnet/test/.glv
```

### Go
```bash
touch gremlin-go/.glv
```

Once `.glv` files are in place, a standard `mvn clean install` will include those GLVs.
The `.glv` files are gitignored.

Alternatively, activate GLV builds explicitly with Maven profiles without `.glv` files:
- Python: `mvn clean install -Pglv-python -pl :gremlin-python`
- .NET: `mvn clean install -Pgremlin-dotnet -pl :gremlin-dotnet,:gremlin-dotnet-source,:gremlin-dotnet-tests`
- Go: `mvn clean install -Pglv-go -pl :gremlin-go`

## Groovy / Ivy Configuration

The Gremlin Console is Groovy-based. For documentation generation or console testing with
SNAPSHOT dependencies, configure `~/.groovy/grapeConfig.xml`:

```xml
<ivysettings>
  <settings defaultResolver="downloadGrapes"/>
  <resolvers>
    <chain name="downloadGrapes">
      <ibiblio name="local" root="file:${user.home}/.m2/repository/" m2compatible="true"/>
      <ibiblio name="central" root="https://repo1.maven.org/maven2/" m2compatible="true"/>
    </chain>
  </resolvers>
</ivysettings>
```

## Docker Permissions (Linux)

If you encounter "Permission denied" errors:

```bash
sudo groupadd docker
sudo usermod -aG docker $USER
newgrp docker
sudo chmod 666 /var/run/docker.sock
```

## IDE Setup

TinkerPop does not mandate a specific IDE. Import the root `pom.xml` as a Maven project.
For AI coding agent configuration, see the "Using AI Coding Agents" section in
`docs/src/dev/developer/development-environment.asciidoc`.

## Next Steps

- See `references/build-java.md` for Java module build details
- See the GLV-specific reference files for language-specific workflows
- See `references/documentation.md` for documentation instructions
