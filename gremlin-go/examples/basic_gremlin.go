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
	"os"

	"github.com/apache/tinkerpop/gremlin-go/v3/driver"
)

var serverURL = getEnv("GREMLIN_SERVER_URL", "ws://localhost:8182/gremlin")
var vertexLabel = getEnv("VERTEX_LABEL", "person")

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	driverRemoteConnection, err := gremlingo.NewDriverRemoteConnection(serverURL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer driverRemoteConnection.Close()
	g := gremlingo.Traversal_().WithRemote(driverRemoteConnection)

	// Basic Gremlin: adding and retrieving data
	v1, err := g.AddV(vertexLabel).Property("name", "marko").Next()
	v2, err := g.AddV(vertexLabel).Property("name", "stephen").Next()
	v3, err := g.AddV(vertexLabel).Property("name", "vadas").Next()
	v1Vertex, err := v1.GetVertex()
	v2Vertex, err := v2.GetVertex()
	v3Vertex, err := v3.GetVertex()

	// Be sure to use a terminating step like Next() or Iterate() so that the traversal "executes"
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
	marko, err := g.V().Has(vertexLabel, "name", "marko").Values("name").Next()
	fmt.Println("name:", marko.GetString())

	// Find the "marko" vertex and then traverse to the people he "knows" and return their data
	peopleMarkoKnows, err := g.V().Has(vertexLabel, "name", "marko").Out("knows").Values("name").ToList()
	for _, person := range peopleMarkoKnows {
		fmt.Println("marko knows", person.GetString())
	}
}
