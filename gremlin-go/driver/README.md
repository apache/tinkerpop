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
# Getting Started

## Prerequisites

* `gremlin-go` requires Golang 1.17 or later, please see [Go Download][go] for more details on installing Golang.
* A basic understanding of [Go Modules][gomods]
* A project set up which uses Go Modules

## Installing the Gremlin-Go as a dependency

To install the Gremlin-Go as a dependency for your project, run the following in the root directory of your project that contains your `go.mod` file:

`go get github.com/apache/tinkerpop/gremlin-go/v3[optionally append @<version>, such as @v3.5.3 - note this requires GO111MODULE=on]`

Available versions can be found at [pkg.go.dev](https://pkg.go.dev/github.com/apache/tinkerpop/gremlin-go/v3/driver?tab=versions).

After running the `go get` command, your `go.mod` file should contain something similar to the following:

```
module gremlin-go-example

go 1.17

require github.com/apache/tinkerpop/gremlin-go/v3 v<version>
```

If it does, then this means Gremlin-Go was successfully installed as a dependency of your project.

You will need to run `go mod tidy` to import the remaining dependencies of the `gremlin-go` driver (if your IDE does not do so automatically), after which you should see an updated `go.mod` file:

```
module gremlin-go-example

go 1.17

require github.com/apache/tinkerpop/gremlin-go v0.0.0-20220131225152-54920637bf94

require (
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.1.2 // indirect
	golang.org/x/text v0.3.7 // indirect
)
```
As well as a populated `go.sum` file.

*if there are no usages for gremlingo found, it will remove the require from go.mod and not import dependencies.*

## Simple usage
For instructions on simple usage, including connecting, connection settings, aliases and more, check out the [Gremlin-Go][tkpop-go-docs] section in the Tinkerpop Documentation.

## Troubleshooting

### Can't establish connection and get any result
* Verify you are using valid server protocol and path. Note that for secure connection `wss` should be used.
* Verify firewall settings.

### Local server doesn't have valid certificate
* Set connection option &tls.Config{InsecureSkipVerify: true}

### Client hangs on requests with large amount of data
* Increase read buffer size by settings connection option `readBufferSize`.

# Gremlin-Go Development

## Design Architecture

See [Gremlin-Go Design Overview](../design.md)

## Building Directly

To build the driver you must install `go`. The following command can be used to build the driver:
`go build <path to source code>`

## Code Styling and Linting
Before generating a pull request, you should manually run the following to ensure correct code styling and fix any issues indicated by the linters.

## Formatting files with Gofmt
To ensure clean and readable code [Gofmt][gofmt] is used.

Navigate to file path in a terminal window and run:

`go fmt`

Gofmt will recursively check for and format `.go` files.

Note: If your IDE of choice is [GoLand][goland], code can be automatically formatted with Gofmt on file save. Instructions on how to enable this feature can be found [here][fmtsave].

## Using the Linter and staticcheck
Run [go vet][gvet] and [staticcheck][scheck] and correct any errors.

[go vet][gvet] is installed when you install go, and can be run with:

`go vet <path to source code>`

Please review the [staticcheck documentation][scheck docs] for more details on installing [staticcheck][scheck]. It can be run with:

`staticcheck <path to source code>`

## Testing with Docker

Docker allows you to test the driver without installing any dependencies. Gremlin Go uses Docker for all of its testing. Please make sure Docker is installed and running on your system. You will need to install both [Docker Engine][dengine] and [Docker Compose][dcompose], which are included in [Docker Desktop][ddesktop].

The docker compose environment variable `GREMLIN_SERVER` specifies the Gremlin server docker image to use, i.e. an image with the tag
`tinkerpop/gremlin-server:$GREMLIN_SERVER`, and is a required environment variable. This also requires the specified docker image to exist,
either locally or in [Docker Hub][dhub].

Running `mvn clean install -pl gremlin-server -DskipTests -DskipIntegrationTests=true -Dci -am` in the main `tinkerpop` directory will automatically build a local SNAPSHOT Gremlin server image. If your OS Platform cannot build a local SNAPSHOT Gremlin server through `maven`, it is recommended to use the latest released server version from [Docker Hub][dhub] (do not use `GREMLIN_SERVER=latest`, use actual version number, e.g. `GREMLIN_SERVER=3.5.x` or `GREMLIN_SERVER=3.6.x`).

The docker compose environment variable `HOME` specifies the user home directory for mounting volumes during test image set up. This variable is set by default in Unix/Linux, but will need to be set for Windows, for example, run `$env:HOME=$env:USERPROFILE` in PowerShell.

There are different ways to launch the test suite and set the `GREMLIN_SERVER` environment variable depending on your Platform:
- Run Maven commands, e.g. `mvn clean install` inside of `tinkerpop/gremlin-go`, or `mvn clean install -pl gremlin-go` inside of `tinkerpop` (platform-agnostic - recommended)
- Run the `run.sh` script, which sets `GREMLIN_SERVER` by default. Run `./run.sh -h` for usage information (Unix/Linux - recommended).
- Add `GREMLIN_SERVER=<server-image-version>` and `HOME=<user-home-directory>` to an `.env` file inside `gremlin-go` and run `docker-compose up --exit-code-from gremlin-go-integration-tests` (Platform-agnostic).
- Run `GREMLIN_SERVER=<server-image-version> docker-compose up --exit-code-from gremlin-go-integration-tests` in Unix/Linux.
- Run `$env:GREMLIN_SERVER="<server-image-version>";$env:HOME=$env:USERPROFILE;docker-compose up --exit-code-from gremlin-go-integration-tests` in Windows PowerShell.

You should see exit code 0 upon successful completion of the test suites. Run `docker-compose down` to remove the service containers (not needed if you executed Maven commands or `run.sh`), or `docker-compose down --rmi all` to remove the service containers while deleting all used images.

[go]: https://go.dev/dl/
[gomods]: https://go.dev/blog/using-go-modules
[gvet]: https://pkg.go.dev/cmd/vet
[scheck]: https://staticcheck.io
[scheck docs]: https://staticcheck.io/docs/getting-started
[gofmt]: https://pkg.go.dev/cmd/gofmt
[goland]: https://www.jetbrains.com/go/
[fmtsave]: https://www.jetbrains.com/help/go/reformat-and-rearrange-code.html#reformat-on-save
[ddesktop]:https://docs.docker.com/desktop/
[dengine]:https://docs.docker.com/engine/install/
[dcompose]:https://docs.docker.com/compose/install/
[dhub]:https://hub.docker.com/r/tinkerpop/gremlin-server
[tkpop-go-docs]:https://tinkerpop.apache.org/docs/current/reference/#gremlin-go