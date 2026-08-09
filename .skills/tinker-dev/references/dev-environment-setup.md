# Development Environment Setup

This guide walks through setting up a TinkerPop development environment from a fresh clone.
The canonical reference is `docs/src/dev/developer/development-environment.asciidoc`.

## Prerequisites

### Minimum (Java-only build)

- **Java 8 or 11** — OpenJDK recommended, with Java 11 preferred: the build cross-compiles to
  Java 8 bytecode. Use [SDKMAN!](https://sdkman.io/) to manage versions. Java 17 also compiles,
  but deep reflection needs the `--add-opens` JVM options already configured in the pom files.
  Use Java 11 for documentation generation with `bin/process-docs.sh`.
- **Maven 3.5.3+** — also manageable via SDKMAN.

With just Java and Maven you can build JVM modules and get a clean `mvn clean install`, but
GLV builds and many integration tests will be skipped.

### Full environment (all GLVs and integration tests)

- **Docker and Docker Compose** — required for all GLV test execution. Docker Desktop includes
  both. GLV tests run inside Docker containers, so local language runtimes are optional for
  test execution via Maven.
- **Python 3.9–3.13** — optional for local development; Docker handles test execution.
- **Node.js 20+ / npm 10+** — optional; Maven downloads a local copy via `frontend-maven-plugin`
  (`node.version` and `npm.version` in the root `pom.xml` pin the exact versions used by the
  build). Consider [nvm](https://github.com/nvm-sh/nvm) for version management.
- **.NET SDK 6.0+** — optional; Docker handles test execution. `Gremlin.Net` targets
  `netstandard2.0` and `net6.0`.
- **Go 1.25+** — optional; Docker handles test execution.

## Environment Verification

Run the included check script to verify your setup:

```bash
bash .skills/tinker-dev/scripts/check-env.sh
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

- See the **Definition of Done** table in `SKILL.md` for the build and validate commands per
  module / GLV.
