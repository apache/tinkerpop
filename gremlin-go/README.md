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
<!-- TODO: Fill this section in with instructions on how to try it out. -->

# Go Gremlin Language Variant

[Apache TinkerPop™][tk] is a graph computing framework for both graph databases (OLTP) and graph analytic systems
(OLAP). [Gremlin][gremlin] is the graph traversal language of TinkerPop. It can be described as a functional,
data-flow language that enables users to succinctly express complex traversals on (or queries of) their application's
property graph.

Gremlin-Go implements Gremlin within the Go language and can be used on any Go runtime greater than v1.17. Go's syntax 
has the same constructs as Java including "dot notation" for function chaining (a.b.c), round bracket function arguments
(a(b,c)), and support for global namespaces (a(b()) vs a(__.b())). One important distinction with Go and Java is that 
the functions are capitalized, as is required to export functions is Go. As such, anyone familiar with Gremlin-Java 
will immediately be able to work with Gremlin-Go.

Gremlin-Go is designed to connect to a "server" that is hosting a TinkerPop-enabled graph system. That "server"
could be [Gremlin Server][gs] or a [remote Gremlin provider][rgp] that exposes protocols by which Gremlin-Go
can connect.

A typical connection to a server running on "localhost" that supports the Gremlin Server protocol using websockets
looks like this:
<!--
TODO: Add Go code example of connection to server.
-->

Once "g" has been created using a connection, it is then possible to start writing Gremlin traversals to query the
remote graph:
<!--
TODO: Add Go code example of a Gremlin traversal query.
-->

# The following material is currently Work-in-progress: 

## Sample Traversals
<!--
TODO: Add Go specific changes to following paragraph:
examples:
"For the most part, these examples should generally translate to Go with [little modification][differences]"
"Given the strong correspondence between canonical Gremlin in Java and its variants like Go, there is a limited amount 
of Go-specific documentation and examples."
-->
The Gremlin language allows users to write highly expressive graph traversals and has a broad list of functions that
cover a wide body of features. The [Reference Documentation][steps] describes these functions and other aspects of the
TinkerPop ecosystem including some specifics on [Gremlin in Go][docs] itself. Most of the examples found in the
documentation use Groovy language syntax in the [Gremlin Console][console]. For the most part, these examples
should generally translate to Go with [little modification][differences]. Given the strong correspondence
between canonical Gremlin in Java and its variants like Go, there is a limited amount of Go-specific
documentation and examples. This strong correspondence among variants ensures that the general Gremlin reference
documentation is applicable to all variants and that users moving between development languages can easily adopt the
Gremlin variant for that language.

### Create Vertex
<!--
TODO: Add Go code to create a vertex.
-->

### Find Vertices
<!--
TODO: Add Go code for Find Vertices.
-->

### Update Vertex
<!--
TODO: Add Go code for Update Vertex.
-->



## Test Coverage

[tk]: https://tinkerpop.apache.org
[gremlin]: https://tinkerpop.apache.org/gremlin.html
[docs]: https://tinkerpop.apache.org/docs/current/reference/#gremlin-go
[gs]: https://tinkerpop.apache.org/docs/current/reference/#gremlin-server
[rgp]: https://tinkerpop.apache.org/docs/current/reference/#connecting-rgp
[console]: https://tinkerpop.apache.org/docs/current/tutorials/the-gremlin-console/
[steps]: https://tinkerpop.apache.org/docs/current/reference/#graph-traversal-steps
[differences]: https://tinkerpop.apache.org/docs/current/reference/#gremlin-go-differences