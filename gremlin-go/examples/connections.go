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
var vertexLabel = getEnv("VERTEX_LABEL", "connection")

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	withRemote()
	withConfigs()
}

func withRemote() {
	// Creating the connection to the server
	driverRemoteConnection, err := gremlingo.NewDriverRemoteConnection(serverURL)

	// Error handling
	if err != nil {
		fmt.Println(err)
		return
	}

	// Cleanup
	defer driverRemoteConnection.Close()

	// Creating the graph traversal
	g := gremlingo.Traversal_().WithRemote(driverRemoteConnection)

	// Simple query to verify connection
	g.AddV(vertexLabel).Iterate()
	count, _ := g.V().HasLabel(vertexLabel).Count().Next()
	fmt.Println("Vertex count:", *count)
}

func withConfigs() {
	// Connecting to the server with customized configurations
	driverRemoteConnection, err := gremlingo.NewDriverRemoteConnection(serverURL,
		func(settings *gremlingo.DriverRemoteConnectionSettings) {
			settings.TraversalSource = "g"
			settings.NewConnectionThreshold = 4
			settings.EnableCompression = false
			settings.ReadBufferSize = 0
			settings.WriteBufferSize = 0
		})

	if err != nil {
		fmt.Println(err)
		return
	}

	defer driverRemoteConnection.Close()
	g := gremlingo.Traversal_().WithRemote(driverRemoteConnection)

	g.AddV(vertexLabel).Iterate()
	count, _ := g.V().HasLabel(vertexLabel).Count().Next()
	fmt.Println("Vertex count:", *count)
}
