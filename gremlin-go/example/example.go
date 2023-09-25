/*
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
*/

package main

import (
	"fmt"

	"github.com/apache/tinkerpop/gremlin-go/v3/driver"
)

// syntactic sugar
var __ = gremlingo.T__
var gt = gremlingo.P.Gt
var order = gremlingo.Order
var T = gremlingo.T
var P = gremlingo.P

func main() {
	connectionExample()
	basicGremlinExample()
	modernTraversalExample()
}

func connectionExample() {
	// Creating the connection to the server
	driverRemoteConnection, err := gremlingo.NewDriverRemoteConnection("ws://localhost:8182/gremlin")

	// Connecting to the server with customized configurations
	driverRemoteConnection, err = gremlingo.NewDriverRemoteConnection("ws://localhost:8182/gremlin",
		func(settings *gremlingo.DriverRemoteConnectionSettings) {
			settings.TraversalSource = "g"
			settings.NewConnectionThreshold = 4
			settings.EnableCompression = false
			settings.ReadBufferSize = 0
			settings.WriteBufferSize = 0
		})

	// Error handling
	if err != nil {
		fmt.Println(err)
		return
	}

	// Cleanup
	defer driverRemoteConnection.Close()

	// Creating the graph traversal
	g := gremlingo.Traversal_().WithRemote(driverRemoteConnection)
	fmt.Println(g)
}

func basicGremlinExample() {
	driverRemoteConnection, err := gremlingo.NewDriverRemoteConnection("ws://localhost:8182/gremlin")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer driverRemoteConnection.Close()
	g := gremlingo.Traversal_().WithRemote(driverRemoteConnection)

	// Basic Gremlin: adding and retrieving data
	v1, err := g.AddV("person").Property("name", "marko").Next()
	v2, err := g.AddV("person").Property("name", "stephen").Next()
	v3, err := g.AddV("person").Property("name", "vadas").Next()
	v1Vertex, err := v1.GetVertex()
	v2Vertex, err := v2.GetVertex()
	v3Vertex, err := v3.GetVertex()

	// Be sure to use a terminating step like next() or iterate() so that the traversal "executes"
	// Iterate() does not return any data and is used to just generate side-effects (i.e. write data to the database)
	promise := g.V(v1Vertex).AddE("knows").To(v2Vertex).Property("weight", 0.75).Iterate()
	err = <-promise
	if err != nil {
		fmt.Println(err)
		return
	}
	promise = g.V(v1Vertex).AddE("knows").To(v3Vertex).Property("weight", 0.75).Iterate()
	err = <-promise
	if err != nil {
		fmt.Println(err)
		return
	}

	// Retrieve the data from the "marko" vertex
	marko, err := g.V().Has("person", "name", "marko").Values("name").Next()
	fmt.Println("name:", marko.GetString())

	// Find the "marko" vertex and then traverse to the people he "knows" and return their data
	peopleMarkoKnows, err := g.V().Has("person", "name", "marko").Out("knows").Values("name").ToList()
	for _, person := range peopleMarkoKnows {
		fmt.Println("marko knows", person.GetString())
	}
}

func modernTraversalExample() {
	driverRemoteConnection, err := gremlingo.NewDriverRemoteConnection("ws://localhost:8182/gremlin")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer driverRemoteConnection.Close()
	g := gremlingo.Traversal_().WithRemote(driverRemoteConnection)

	/*
	  This example requires the Modern toy graph to be preloaded upon launching the Gremlin server.
	  For details, see https://tinkerpop.apache.org/docs/current/reference/#gremlin-server-docker-image and use
	  conf/gremlin-server-modern.yaml.
	*/
	e1, err := g.V(1).BothE().ToList()                            // (1)
	e2, err := g.V().BothE().Where(__.OtherV().HasId(2)).ToList() // (2)
	v1, err := g.V(1).Next()
	v2, err := g.V(2).Next()
	v1Vertex, err := v1.GetVertex()
	v2Vertex, err := v2.GetVertex()
	e3, err := g.V(v1Vertex).BothE().Where(__.OtherV().Is(v2Vertex)).ToList()   // (3)
	e4, err := g.V(v1Vertex).OutE().Where(__.InV().Is(v2Vertex)).ToList()       // (4)
	e5, err := g.V(1).OutE().Where(__.InV().Has(T.Id, P.Within(2, 3))).ToList() // (5)
	e6, err := g.V(1).Out().Where(__.In().HasId(6)).ToList()                    // (6)

	fmt.Println(e1)
	fmt.Println(e2)
	fmt.Println(e3)
	fmt.Println(e4)
	fmt.Println(e5)
	fmt.Println(e6)

	/*
	  1. There are three edges from the vertex with the identifier of "1".
	  2. Filter those three edges using the Where()-step using the identifier of the vertex returned by OtherV() to
	     ensure it matches on the vertex of concern, which is the one with an identifier of "2".
	  3. Note that the same traversal will work if there are actual Vertex instances rather than just vertex
	     identifiers.
	  4. The vertex with identifier "1" has all outgoing edges, so it would also be acceptable to use the directional
	     steps of OutE() and InV() since the schema allows it.
	  5. There is also no problem with filtering the terminating side of the traversal on multiple vertices, in this
	     case, vertices with identifiers "2" and "3".
	  6. There’s no reason why the same pattern of exclusion used for edges with Where() can’t work for a vertex
	     between two vertices.
	*/
}
