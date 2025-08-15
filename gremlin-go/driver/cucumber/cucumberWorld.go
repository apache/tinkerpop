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
	gremlingo "github.com/apache/tinkerpop/gremlin-go/v3/driver"
	"github.com/cucumber/godog"
	"os"
	"reflect"
	"strconv"
)

type CucumberWorld struct {
	scenario     *godog.Scenario
	g            *gremlingo.GraphTraversalSource
	graphName    string
	traversal    *gremlingo.GraphTraversal
	result       []interface{}
	error        map[bool]string
	graphDataMap map[string]*DataGraph
	parameters   map[string]interface{}
	sideEffects  map[string]interface{}
}

type DataGraph struct {
	name             string
	connection       *gremlingo.DriverRemoteConnection
	vertices         map[string]*gremlingo.Vertex
	vertexProperties map[string]*gremlingo.VertexProperty
	edges            map[string]*gremlingo.Edge
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
	return getEnvOrDefaultString("GREMLIN_SERVER_URL", "ws://localhost:45940/gremlin")
}

func NewCucumberWorld() *CucumberWorld {
	return &CucumberWorld{
		scenario:     nil,
		g:            nil,
		graphName:    "",
		traversal:    nil,
		result:       nil,
		error:        make(map[bool]string),
		graphDataMap: make(map[string]*DataGraph),
		parameters:   make(map[string]interface{}),
		sideEffects:  make(map[string]interface{}),
	}
}

var graphNames = []string{"modern", "classic", "crew", "grateful", "sink", "empty"}

func (t *CucumberWorld) getDataGraphFromMap(name string) *DataGraph {
	if val, ok := t.graphDataMap[name]; ok {
		return val
	} else {
		return nil
	}
}

func (t *CucumberWorld) loadAllDataGraph() {
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
			g := gremlingo.Traversal_().With(connection)
			t.graphDataMap[name] = &DataGraph{
				name:             name,
				connection:       connection,
				vertices:         getVertices(g),
				vertexProperties: getVertexProperties(g),
				edges:            getEdges(g),
			}
		}
	}
}

func (t *CucumberWorld) loadEmptyDataGraph() {
	connection, _ := gremlingo.NewDriverRemoteConnection(scenarioUrl(), func(settings *gremlingo.DriverRemoteConnectionSettings) {
		settings.TraversalSource = "ggraph"
	})
	t.graphDataMap["empty"] = &DataGraph{connection: connection}
}

func (t *CucumberWorld) reloadEmptyData() {
	graphData := t.getDataGraphFromMap("empty")
	g := gremlingo.Traversal_().With(graphData.connection)
	graphData.vertices = getVertices(g)
	graphData.edges = getEdges(g)
}

func (t *CucumberWorld) cleanEmptyDataGraph(g *gremlingo.GraphTraversalSource) error {
	future := g.V().Drop().Iterate()
	return <-future
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

func getVertexProperties(g *gremlingo.GraphTraversalSource) map[string]*gremlingo.VertexProperty {
	vertexPropertyMap := make(map[string]*gremlingo.VertexProperty)
	res, err := g.V().Properties().Group().By(&gremlingo.Lambda{
		Script: "{ it -> \n" +
			"  def val = it.value()\n" +
			"  if (val instanceof Integer)\n" +
			"    val = 'd[' + val + '].i'\n" +
			"  else if (val instanceof Float)\n" +
			"    val = 'd[' + val + '].f'\n" +
			"  else if (val instanceof Double)\n" +
			"    val = 'd[' + val + '].d'\n" +
			"  return it.element().value('name') + '-' + it.key() + '->' + val\n" +
			"}",
		Language: "",
	}).By(gremlingo.T__.Tail()).Next()
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
		vertexPropertyMap[k.Interface().(string)] = val.Interface().(*gremlingo.VertexProperty)
	}
	return vertexPropertyMap
}

// This function is used to isolate connection problems to each scenario, and used in the Before context hook to prevent
// a failing test in one scenario closing the shared connection that leads to failing subsequent scenario tests.
// This function can be removed once all pending tests pass.
func (t *CucumberWorld) recreateAllDataGraphConnection() error {
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

func (t *CucumberWorld) closeAllDataGraphConnection() error {
	for _, name := range graphNames {
		t.getDataGraphFromMap(name).connection.Close()
	}
	return nil
}
