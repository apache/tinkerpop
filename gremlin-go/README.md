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
NOTE that versions suffixed with "-rc" are considered release candidates (i.e. pre-alpha, alpha, beta, etc.) and thus
for early testing purposes only.

[![codecov](https://codecov.io/gh/apache/tinkerpop/branch/master/graph/badge.svg?token=TojD2nR5Qd)](https://codecov.io/gh/apache/tinkerpop)

# Go Gremlin Language Variant

[Apache TinkerPop™][tk] is a graph computing framework for both graph databases (OLTP) and graph analytic systems
(OLAP). [Gremlin][gremlin] is the graph traversal language of TinkerPop. It can be described as a functional,
data-flow language that enables users to succinctly express complex traversals on (or queries of) their application's
property graph.

Gremlin-Go implements Gremlin within the Go language and can be used on any Go runtime greater than v1.21. One
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
	g := gremlingo.Traversal_().With(driverRemoteConnection)

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
results, err := g.V().HasLabel("person").Has("age", gremlingo.T__.Is(gremlingo.P.Gt(30))).Order().By("age", gremlingo.Order.Desc).ToList()
```

Or with aliases
```go
results, err := g.V().HasLabel("person").Has("age", __.Is(gt(30))).Order().By("age", order.Desc).ToList()
```

*List of all exports can be found at [pkg.go.dev](https://pkg.go.dev/github.com/apache/tinkerpop/gremlin-go/v3/driver)*

### Supported Data Types
The `Go` driver supports all of the core GraphBinary data types.

### More
For information on Installation, Connection Usage, Developer Documentation and more, visit the [driver docs](driver/README.md)

[tk]: https://tinkerpop.apache.org
[gremlin]: https://tinkerpop.apache.org/gremlin.html
[docs]: https://tinkerpop.apache.org/docs/current/reference/#gremlin-go
[gs]: https://tinkerpop.apache.org/docs/current/reference/#gremlin-server
[rgp]: https://tinkerpop.apache.org/docs/current/reference/#connecting-rgp
[console]: https://tinkerpop.apache.org/docs/current/tutorials/the-gremlin-console/
[steps]: https://tinkerpop.apache.org/docs/current/reference/#graph-traversal-steps
[differences]: https://tinkerpop.apache.org/docs/current/reference/#gremlin-go-differences
