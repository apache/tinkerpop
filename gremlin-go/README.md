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

# Getting Started

## Building the Source Code

If you would like to build and/or test the source code, you can do so with docker or directly with the go compiler, the instructions for both are listed below.

### Testing with Docker

Docker allows you to test the driver without installing any dependencies. The following command can be used to run docker:

`docker-compose up --exit-code-from integration-tests integration-tests`

### Building Directly

To build the driver you must install `go`. The following command can be used to build the driver:
`go build <path to source code>`

### Code Styling and Linting
Before generating a pull request, you should manually run the following to ensure correct code styling and fix any issues indicated by the linters.

### Formatting files with Gofmt
To ensure clean and readable code [Gofmt][gofmt] is used. 

Navigate to file path in a terminal window and run:

`gofmt -s -w` 

Gofmt will recursively check for and format `.go` files.

Note: If your IDE of choice is [GoLand][goland], code can be automatically formatted with Gofmt on file save. Instructions on how to enable this feature can be found [here][fmtsave].

### Using the Linter and staticcheck
Run [go vet][gvet] and [staticcheck][scheck] and correct any errors.

[go vet][gvet] is installed when you install go, and can be run with:

 `go vet <path to source code>`

Please review the [staticcheck documentation][scheck docs] for more details on installing [staticcheck][scheck]. It can be run with:

`staticcheck <path to source code>`

### Prerequisites

* `gremlin-go` requires Golang 1.17 or later, please see [Go Download][go] for more details on installing Golang.
* A basic understanding of [Go Modules][gomods]
* A project set up which uses Go Modules

To install the Gremlin-Go as a dependency for your project, run the following in the root directory of your project that contains your `go.mod` file:

`go get github.com/lyndonb-bq/tinkerpop/gremlin-go@gremlin-go-ms2`

Note: Currently as of Milestone #2, Gremlin-Go exists in the `lyndonb-bq` fork on the `gremlin-go-ms2` branch. Expect this to change in the future when the project is closer to a completed state.

After running the `go get` command, your `go.mod` file should contain something similar to the following:

```
module gremlin-go-example

go 1.17

require github.com/lyndonb-bq/tinkerpop/gremlin-go v0.0.0-20220131225152-54920637bf94
```

If it does, then this means Gremlin-Go was successfully installed as a dependency of your project.

Here is a simple example of using Gremlin-Go as an import in a sample project's `main.go` file. 
```go
package main

import (
	"github.com/lyndonb-bq/tinkerpop/gremlin-go/driver"
)

func main() {
	// Simple stub to use the import. See subsequent section for actual usage. 
	_, _ = gremlingo.NewDriverRemoteConnection("localhost", 8182)
}
```
You will need to run `go mod tidy` to import the remaining dependencies of the `gremlin-go` driver (if your IDE does not do so automatically), after which you should see an updated `go.mod` file:

```
module gremlin-go-example

go 1.17

require github.com/lyndonb-bq/tinkerpop/gremlin-go v0.0.0-20220131225152-54920637bf94

require (
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.1.2 // indirect
	golang.org/x/text v0.3.7 // indirect
)
```
As well as a populated `go.sum` file.

This following example should run, provided that it is configured to point to a compatible `gremlin-server`. In this example, a simple local server is running on port 8182, and this will print`[2]` as an output. If no server is available, this code can still be executed to print an error as output.

```go
package main

import (
	"fmt"
	"github.com/lyndonb-bq/tinkerpop/gremlin-go/driver"
)

func main() {
	// Creating the connection to the server.
	driverRemoteConnection, err := gremlingo.NewDriverRemoteConnection("localhost", 8182)
	if err != nil {
		fmt.Println(err)
		return
	}
	// Cleanup
	defer driverRemoteConnection.Close()
	
	// Submit a simple string traversal
	resultSet, err := driverRemoteConnection.Submit("1 + 1")
	if err != nil {
		fmt.Println(err)
		return
	}
	
	// Grab the first result from all the results in the ResultSet.
	result := resultSet.All()[0]
	
	// Print the first result.
	fmt.Println(result.GetString())
}
```

Note: The exact import name as well as the module prefix for `NewDriverRemoteConnection` may change in the future.

### Design Architecture

See [Gremlin-Go Design Overview](design.MD)

# Go Gremlin Language Variant

[Apache TinkerPop™][tk] is a graph computing framework for both graph databases (OLTP) and graph analytic systems
(OLAP). [Gremlin][gremlin] is the graph traversal language of TinkerPop. It can be described as a functional,
data-flow language that enables users to succinctly express complex traversals on (or queries of) their application's
property graph.

Gremlin-Go implements Gremlin within the Go language and can be used on any Go runtime greater than v1.17. One
important distinction with Go and Java is that the functions are capitalized, as is required to export functions is Go. 

Gremlin-Go is designed to connect to a "server" that is hosting a TinkerPop-enabled graph system. That "server"
could be [Gremlin Server][gs] or a [remote Gremlin provider][rgp] that exposes protocols by which Gremlin-Go
can connect.

A typical connection to a server running on "localhost" that supports the Gremlin Server protocol using websockets
looks like this:
```go
package main

import (
	"fmt"
	"github.com/lyndonb-bq/tinkerpop/gremlin-go/driver"
)

func main() {
	// Creating the connection to the server with default settings.
	driverRemoteConnection, err := gremlingo.NewDriverRemoteConnection("localhost", 8182)
	// Handle error
	if err != nil {
		fmt.Println(err)
		return
	}
	// Create an anonymous traversal source with remote
	g := gremlingo.Traversal_().WithRemote(driverRemoteConnection)
}
```
We can also customize the remote connection settings. (See code documentation for additional parameters and their usage).
```go
package main

import (
	"fmt"
	"github.com/lyndonb-bq/tinkerpop/gremlin-go/driver"
)

func main() {
	// Creating the connection to the server, changing the log verbosity to Debug.
	driverRemoteConnection, err := gremlingo.NewDriverRemoteConnection("localhost", 8182, func(settings *gremlingo.DriverRemoteConnectionSettings) {
		settings.LogVerbosity = gremlingo.Debug
	})
	// Handle error
	if err != nil {
		fmt.Println(err)
		return
	}
	// Create an anonymous traversal source with remote
	g := gremlingo.Traversal_().WithRemote(driverRemoteConnection)
	// Use g with traversal as detailed in the next section.
}
```
Once "g" has been created using a connection, it is then possible to start writing Gremlin traversals to query the
remote graph:
```go
package main

import (
	"fmt"
	"github.com/lyndonb-bq/tinkerpop/gremlin-go/driver"
)

func main() {
	// Creating the connection to the server.
	driverRemoteConnection, err := gremlingo.NewDriverRemoteConnection("localhost", 8182)
	// Handle error
	if err != nil {
		fmt.Println(err)
		return
	}
	// Create an anonymous traversal source with remote
	g := gremlingo.Traversal_().WithRemote(driverRemoteConnection)

	// Add a vertex with properties to the graph with the terminal step Iterate()
	_, promise, err := g.AddV("gremlin").Property("language", "go").Iterate()
	if err != nil {
		fmt.Println(err)
		return 
	}
	// The returned promised is a go channel, to wait for all submitted steps to finish execution.
	<-promise
	
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
<!--
TODO: Add Go specific changes to following paragraph:
-->
The Gremlin language allows users to write highly expressive graph traversals and has a broad list of functions that
cover a wide body of features. Traversal in `Go` is very similar to other GLV, with the exception that all step functions are capitalized.

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
_, promise, err := g.AddV("gremlin").Property("language", "go").Iterate()
// Handle error
if err != nil {
    fmt.Println(err)
    return
}
// Wait for all steps to finish execution
<-promise
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
_, promise, err :=g.AddV("gremlin").Property("language", "go").Iterate()
// Handle error
if err != nil {
	fmt.Println(err)
    return
}
// Wait for all steps to finish execution
<-promise
```

# Specifications
### Supported Data Types
The current `go` driver supports the following GraphBinary serialization types. 
```
NullType            DataType = 0xFE
IntType             DataType = 0x01
LongType            DataType = 0x02
StringType          DataType = 0x03
DoubleType          DataType = 0x07
FloatType           DataType = 0x08
ListType            DataType = 0x09
MapType             DataType = 0x0a
UUIDType            DataType = 0x0c
BytecodeType        DataType = 0x15
TraverserType       DataType = 0x21
ByteType            DataType = 0x24
ShortType           DataType = 0x26
BooleanType         DataType = 0x27
BigIntegerType      DataType = 0x23
VertexType          DataType = 0x11
EdgeType            DataType = 0x0d
PropertyType        DataType = 0x0f
VertexPropertyType  DataType = 0x12
PathType            DataType = 0x0e     // see limitations
SetType             DataType = 0x0b     // see limitations
```
### Current Limitations
- The `set` data type is currently not implemented, as `go` does not have an underlying `set` data structure. Any `set` type code from server will be deserialized into `slices` with the `list` type implementation. Set implemented with the `go` convention of using a `map` will be serialized as `map`.
- The `path` data type serialization is currently incomplete as labels are represented as list of lists instead of list of sets. Fully functional Path serialization will be implemented when `set` is implemented in the next milestone. `Path` can be successfully deserialized. 
- Traversal step functions currently take `string` arguments with double quotes only. Operations using Gremlin keywords, such as `By(label)`, will be supported in the next milestone. 
## Test Coverage

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