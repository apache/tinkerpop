NOTE that versions suffixed with "-rc" are considered release candidates (i.e. pre-alpha, alpha, beta, etc.) and thus
for early testing purposes only.

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

[![codecov](https://codecov.io/gh/apache/tinkerpop/branch/master/graph/badge.svg?token=TojD2nR5Qd)](https://codecov.io/gh/apache/tinkerpop)

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

Here is a simple example of using Gremlin-Go as an import in a sample project's `main.go` file. 
```go
package main

import (
	"github.com/apache/tinkerpop/gremlin-go/driver"
)

func main() {
	// Simple stub to use the import. See subsequent section for actual usage. 
	_, _ = gremlingo.NewDriverRemoteConnection(fmt.Sprintf("ws://%s:%d/gremlin", "localhost", 8182))
}
```

*Pay attention to the suffix `/gremlin` in connection string*

This following example should run, provided that it is configured to point to a compatible `gremlin-server`. In this example, a simple local server is running on port 8182, and this will print `[0]` as an output. If no server is available, this code can still be executed to print an error as output.

```go
package main

import (
	"fmt"
	"github.com/apache/tinkerpop/gremlin-go/v3/driver"
)

func main() {
	// Creating the connection to the server.
	driverRemoteConnection, err := gremlingo.NewDriverRemoteConnection("ws://localhost:8182/gremlin")
	// Handle error
	if err != nil {
		fmt.Println(err)
		return
	}
	// Cleanup
	defer driverRemoteConnection.Close()

	// Creating graph traversal
	g := gremlingo.Traversal_().WithRemote(driverRemoteConnection)

	// Perform traversal
	result, err := g.V().Count().ToList()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(result[0].GetString())
}
```

Note: The exact import name as well as the module prefix for `NewDriverRemoteConnection` may change in the future.

## Customizing connection
`gremlingo.NewDriverRemoteConnection` accepts a config function as a parameter. (See code documentation for additional parameters and their usage)
```go
	driverRemoteConnection, err := gremlingo.NewDriverRemoteConnection("wss://localhost:8182/gremlin",
		func(settings *gremlingo.DriverRemoteConnectionSettings) {
			settings.TlsConfig = &tls.Config{InsecureSkipVerify: true}         // skip certificate validation, only for development
			settings.AuthInfo = gremlingo.BasicAuthInfo("stephen", "password") // use auth
		})
```

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

See [Gremlin-Go Design Overview](design.MD)

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

Docker allows you to test the driver without installing any dependencies. The following command can be used to run docker:

`docker-compose up --exit-code-from integration-tests`

# Go Gremlin Language Variant

[Apache TinkerPop™][tk] is a graph computing framework for both graph databases (OLTP) and graph analytic systems
(OLAP). [Gremlin][gremlin] is the graph traversal language of TinkerPop. It can be described as a functional,
data-flow language that enables users to succinctly express complex traversals on (or queries of) their application's
property graph.

Gremlin-Go implements Gremlin within the Go language and can be used on any Go runtime greater than v1.17. One
important difference between Go and Java is that the functions are capitalized, as is required to export functions is Go. 

Gremlin-Go is designed to connect to a "server" that is hosting a TinkerPop-enabled graph system. That "server"
could be [Gremlin Server][gs] or a [remote Gremlin provider][rgp] that exposes protocols by which Gremlin-Go
can connect.

Once "g" has been created using a connection, it is then possible to start writing Gremlin traversals to query the
remote graph:
```go
package main

import (
	"fmt"
	"github.com/apache/tinkerpop/gremlin-go/v3/driver"
)

func main() {
	// Creating the connection to the server with default settings.
	driverRemoteConnection, err := gremlingo.NewDriverRemoteConnection("ws://localhost:8182/gremlin")
	// Handle error
	if err != nil {
		fmt.Println(err)
		return
	}
	// Cleanup
	defer driverRemoteConnection.Close()

	// Create an anonymous traversal source with remote
	g := gremlingo.Traversal_().WithRemote(driverRemoteConnection)

	// Add a vertex with properties to the graph with the terminal step Iterate()
	promise := g.AddV("gremlin").Property("language", "go").Iterate()

	// The returned promised is a go channel to wait for all submitted steps to finish execution and return error.
	err = <-promise
	if err != nil {
		fmt.Println(err)
		return
	}

	// Get the value of the property
	result, err := g.V().HasLabel("gremlin").Values("language").ToList()
	if err != nil {
		fmt.Println(err)
		return
	}
	// Print the result
	for _, r := range result {
		fmt.Println(r.GetString())
	}
}
```

## Sample Traversals
The Gremlin language allows users to write highly expressive graph traversals and has a broad list of functions that
cover a wide body of features. Traversal in `Go` is very similar to other GLV, with the exception that all step functions are capitalized.

Anonymous traversal `__` replaced with `T__` due to `Go` limitations.

Anything in the package when referenced needs the prefix `gremlingo` like `gremlingo.Desc`.

<!--
The [Reference Documentation][steps] describes these functions and other aspects of the
TinkerPop ecosystem including some specifics on [Gremlin in Go][docs] itself. Most of the examples found in the
documentation use Groovy language syntax in the [Gremlin Console][console]. For the most part, these examples
should generally translate to Go with [little modification][differences]. Given the strong correspondence
between canonical Gremlin in Java and its variants like Go, there is a limited amount of Go-specific
documentation and examples. This strong correspondence among variants ensures that the general Gremlin reference
documentation is applicable to all variants and that users moving between development languages can easily adopt the
Gremlin variant for that language.
-->

### Create Vertex
Adding a vertex with properties.
```go
promise := g.AddV("gremlin").Property("language", "go").Iterate()
// Wait for all steps to finish execution and check for error.
err := <-promise
if err != nil {
    fmt.Println(err)
    return
}
```
### Find Vertices
Getting the property value associated with the added vertex. We currently only support `ToList()` for submitting the remote traversal. Support for `Next()` will be implemented in the subsequent milestones. 
```go
result, err := g.V().HasLabel("gremlin").Values("language").ToList()
// Handle error
if err != nil {
    fmt.Println(err)
    return
}
// Print result
for _, r := range result {
    fmt.Println(r.GetString())
}
```

### Update Vertex
Updating vertex by adding another property to it. 
```go
promise := g.AddV("gremlin").Property("language", "go").Iterate()
// Wait for all steps to finish execution and check for error.
err := <-promise
if err != nil {
    fmt.Println(err)
    return
}
```

### Filtering and sorting
```go
	results, err := g.V().HasLabel("person").Has("age", gremlingo.T__.Is(gremlingo.P.Gt(30))).Order().By("age", gremlingo.Desc).ToList()
```

*List of all exports can be found at [pkg.go.dev](https://pkg.go.dev/github.com/apache/tinkerpop/gremlin-go/v3/driver)*

### Supported Data Types
The `Go` driver supports all of the core GraphBinary data types.

[tk]: https://tinkerpop.apache.org
[gremlin]: https://tinkerpop.apache.org/gremlin.html
[docs]: https://tinkerpop.apache.org/docs/current/reference/#gremlin-go
[gs]: https://tinkerpop.apache.org/docs/current/reference/#gremlin-server
[rgp]: https://tinkerpop.apache.org/docs/current/reference/#connecting-rgp
[console]: https://tinkerpop.apache.org/docs/current/tutorials/the-gremlin-console/
[steps]: https://tinkerpop.apache.org/docs/current/reference/#graph-traversal-steps
[differences]: https://tinkerpop.apache.org/docs/current/reference/#gremlin-go-differences
[go]: https://go.dev/dl/
[gomods]: https://go.dev/blog/using-go-modules
[gvet]: https://pkg.go.dev/cmd/vet
[scheck]: https://staticcheck.io
[scheck docs]: https://staticcheck.io/docs/getting-started
[gofmt]: https://pkg.go.dev/cmd/gofmt
[goland]: https://www.jetbrains.com/go/
[fmtsave]: https://www.jetbrains.com/help/go/reformat-and-rearrange-code.html#reformat-on-save
