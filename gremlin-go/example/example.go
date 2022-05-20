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
	gremlingo "github.com/apache/tinkerpop/gremlin-go/driver"
	"github.com/apache/tinkerpop/gremlin-go/v3/driver"
)

// syntactic sugar
var __ = gremlingo.T__
var gt = gremlingo.P.Gt
var order = gremlingo.Order

func main() {
	// Creating the connection to the server.
	driverRemoteConnection, err := gremlingo.NewDriverRemoteConnection("ws://localhost:8182/gremlin",
		func(settings *gremlingo.DriverRemoteConnectionSettings) {
			settings.TraversalSource = "gmodern"
		})
	if err != nil {
		fmt.Println(err)
		return
	}
	// Cleanup
	defer driverRemoteConnection.Close()

	// Creating graph traversal
	g := gremlingo.Traversal_().WithRemote(driverRemoteConnection)

	// Perform traversal
	result, err := g.V().HasLabel("person").Has("age", __.Is(gt(28))).Order().By("age", order.Desc).Values("name").ToList()
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, r := range result {
		fmt.Println(r.GetString())
	}
}
