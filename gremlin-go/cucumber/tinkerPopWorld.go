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

package gremlingo

import (
	"fmt"
	"github.com/cucumber/godog"
	gremlingo "github.com/lyndonb-bq/tinkerpop/gremlin-go/driver"
	"os"
	"reflect"
	"strconv"
)

type TinkerPopWorld struct {
	scenario     *godog.Scenario
	g            *gremlingo.GraphTraversalSource
	graphName    string
	traversal    *gremlingo.GraphTraversal
	result       []interface{}
	graphDataMap map[string]*DataGraph
	parameters   map[string]interface{}
}

type DataGraph struct {
	name       string
	connection *gremlingo.DriverRemoteConnection
	vertices   map[string]*gremlingo.Vertex
	edges      map[string]*gremlingo.Edge
}

func getEnvOrDefaultString(key string, defaultValue string) string {
	// Missing value is returned as "".
	value := os.Getenv(key)
	if len(value) != 0 {
		return value
	}
	return defaultValue
}

func getEnvOrDefaultInt(key string, defaultValue int) int {
	value := getEnvOrDefaultString(key, "")
	if len(value) != 0 {
		intValue, err := strconv.Atoi(value)
		if err == nil {
			return intValue
		}
	}
	return defaultValue
}

func scenarioUrl() string {
	return getEnvOrDefaultString("GREMLIN_SERVER_URL", "ws://localhost:8182/gremlin")
}

func NewTinkerPopWorld() *TinkerPopWorld {
	return &TinkerPopWorld{
		scenario:     nil,
		g:            nil,
		graphName:    "",
		traversal:    nil,
		result:       nil,
		graphDataMap: make(map[string]*DataGraph),
		parameters:   make(map[string]interface{}),
	}
}

var graphNames = []string{"modern", "classic", "crew", "grateful", "sink", "empty"}

func (t *TinkerPopWorld) getDataGraphFromMap(name string) *DataGraph {
	if val, ok := t.graphDataMap[name]; ok {
		return val
	} else {
		return nil
	}
}

func (t *TinkerPopWorld) loadAllDataGraph() {
	for _, name := range graphNames {
		if name == "empty" {
			t.loadEmptyDataGraph()
		} else {
			connection, err := gremlingo.NewDriverRemoteConnection(scenarioUrl(),
				func(settings *gremlingo.DriverRemoteConnectionSettings) {
					settings.TraversalSource = "g" + name
				})
			if err != nil {
				panic(fmt.Sprintf("Failed to create connection '%v'", err))
			}
			g := gremlingo.Traversal_().WithRemote(connection)
			t.graphDataMap[name] = &DataGraph{
				name:       name,
				connection: connection,
				vertices:   getVertices(g),
				edges:      getEdges(g),
			}
		}
	}
}

func (t *TinkerPopWorld) loadEmptyDataGraph() {
	if _, ok := t.graphDataMap["empty"]; ok {
		return
	}

	connection, _ := gremlingo.NewDriverRemoteConnection(scenarioUrl(), func(settings *gremlingo.DriverRemoteConnectionSettings) {
		settings.TraversalSource = "ggraph"
	})
	t.graphDataMap["empty"] = &DataGraph{connection: connection}
}

func (t *TinkerPopWorld) reloadEmptyData() {
	graphData := t.getDataGraphFromMap("empty")
	g := gremlingo.Traversal_().WithRemote(graphData.connection)
	graphData.vertices = getVertices(g)
	graphData.edges = getEdges(g)
}

func (t *TinkerPopWorld) cleanEmptyDataGraph(g *gremlingo.GraphTraversalSource) error {
	_, future, err := g.V().Drop().Iterate()
	if err != nil {
		return err
	}
	<-future
	return nil
}

func getVertices(g *gremlingo.GraphTraversalSource) map[string]*gremlingo.Vertex {
	vertexMap := make(map[string]*gremlingo.Vertex)
	res, err := g.V().Group().By("name").By(gremlingo.T__.Tail()).Next()
	if res == nil {
		return nil
	}
	if err != nil {
		return nil
	}
	v := reflect.ValueOf(res.GetInterface())
	if v.Kind() != reflect.Map {
		fmt.Printf("Expecting to get a map as a result, got %v instead.", v.Kind())
		return nil
	}
	keys := v.MapKeys()
	for _, k := range keys {
		convKey := k.Convert(v.Type().Key())
		val := v.MapIndex(convKey)
		vertexMap[k.Interface().(string)] = val.Interface().(*gremlingo.Vertex)
	}
	return vertexMap
}

func getEdges(g *gremlingo.GraphTraversalSource) map[string]*gremlingo.Edge {
	edgeMap := make(map[string]*gremlingo.Edge)
	resE, err := g.E().Group().By(gremlingo.T__.Project("o", "l", "i").
		By(gremlingo.T__.OutV().Values("name")).By(gremlingo.T__.Label()).By(gremlingo.T__.InV().Values("name"))).
		By(gremlingo.T__.Tail()).Next()
	if err != nil {
		return nil
	}
	valMap := reflect.ValueOf(resE.GetInterface())
	if valMap.Kind() != reflect.Map {
		fmt.Printf("Expecting to get a map as a result, got %v instead.", valMap.Kind())
		return nil
	}
	keys := valMap.MapKeys()
	for _, k := range keys {
		convKey := k.Convert(valMap.Type().Key())
		val := valMap.MapIndex(convKey)
		keyMap := reflect.ValueOf(k.Interface()).Elem().Interface().(map[interface{}]interface{})
		edgeMap[getEdgeKey(keyMap)] = val.Interface().(*gremlingo.Edge)
	}
	return edgeMap
}

func getEdgeKey(edgeKeyMap map[interface{}]interface{}) string {
	return fmt.Sprint(edgeKeyMap["o"], "-", edgeKeyMap["l"], "->", edgeKeyMap["i"])
}

// This function is used to isolate connection problems to each scenario, and used in the Before context hook to prevent
// a failing test in one scenario closing the shared connection that leads to failing subsequent scenario tests.
// This function can be removed once all pending tests pass.
func (t *TinkerPopWorld) recreateAllDataGraphConnection() error {
	var err error
	for _, name := range graphNames {
		if name == "empty" {
			t.getDataGraphFromMap(name).connection, err = gremlingo.NewDriverRemoteConnection(scenarioUrl(), func(settings *gremlingo.DriverRemoteConnectionSettings) {
				settings.TraversalSource = "ggraph"
			})
		} else {
			t.getDataGraphFromMap(name).connection, err = gremlingo.NewDriverRemoteConnection(scenarioUrl(), func(settings *gremlingo.DriverRemoteConnectionSettings) {
				settings.TraversalSource = "g" + name
			})
		}
	}
	return err
}

func (t *TinkerPopWorld) closeAllDataGraphConnection() error {
	for _, name := range graphNames {
		err := t.getDataGraphFromMap(name).connection.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
