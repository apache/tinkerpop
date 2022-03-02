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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/avarf/getenvs"
	"golang.org/x/text/language"
)

const personLabel = "Person"
const testLabel = "Test"
const nameKey = "name"

func dropGraph(t *testing.T, g *GraphTraversalSource) {
	// Drop vertices that were added.
	_, promise, err := g.V().Drop().Iterate()
	assert.Nil(t, err)
	assert.NotNil(t, promise)
	<-promise
}

func getTestNames() []string {
	return []string{"Lyndon", "Yang", "Simon", "Rithin", "Alexey"}
}

func addTestData(t *testing.T, g *GraphTraversalSource) {
	testNames := getTestNames()

	// Add vertices to traversal.
	var traversal *GraphTraversal
	for _, name := range testNames {
		if traversal == nil {
			traversal = g.AddV(personLabel).Property(nameKey, name)
		} else {
			traversal = traversal.AddV(personLabel).Property(nameKey, name)
		}
	}

	// Commit traversal.
	_, promise, err := traversal.Iterate()
	assert.Nil(t, err)
	<-promise
}

func readTestDataVertexProperties(t *testing.T, g *GraphTraversalSource) {
	// Read names from graph
	var sortedNames []string
	results, err := g.V().HasLabel(personLabel).Properties(nameKey).ToList()
	for _, result := range results {
		vp, err := result.GetVertexProperty()
		assert.Nil(t, err)
		sortedNames = append(sortedNames, vp.value.(string))
	}
	assert.Nil(t, err)
	assert.NotNil(t, sortedNames)

	// Sort names on both sides.
	testNames := getTestNames()
	sort.Slice(sortedNames, func(i, j int) bool {
		return sortedNames[i] < sortedNames[j]
	})
	sort.Slice(testNames, func(i, j int) bool {
		return testNames[i] < testNames[j]
	})
	assert.Equal(t, sortedNames, testNames)
}

func readTestDataValues(t *testing.T, g *GraphTraversalSource) {
	// Read names from graph
	var sortedNames []string
	results, err := g.V().HasLabel(personLabel).Values(nameKey).ToList()
	for _, result := range results {
		sortedNames = append(sortedNames, result.GetString())
	}
	assert.Nil(t, err)
	assert.NotNil(t, sortedNames)

	// Sort names on both sides.
	testNames := getTestNames()
	sort.Slice(sortedNames, func(i, j int) bool {
		return sortedNames[i] < sortedNames[j]
	})
	sort.Slice(testNames, func(i, j int) bool {
		return testNames[i] < testNames[j]
	})
	assert.Equal(t, sortedNames, testNames)
}

func readCount(t *testing.T, g *GraphTraversalSource, label string, expected int) {
	// Generate traversal.
	var traversal *GraphTraversal
	if label != "" {
		traversal = g.V().HasLabel(label).Count()
	} else {
		traversal = g.V().Count()
	}

	// Get results from traversal.
	results, err := traversal.ToList()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))

	// Read count from results.
	var count int32
	count, err = results[0].GetInt32()
	assert.Nil(t, err)

	// Check count.
	assert.Equal(t, int32(expected), count)
}

func readUsingAnonymousTraversal(t *testing.T, g *GraphTraversalSource) {
	results, err := g.V().Fold().
		Project(testLabel, personLabel).
			By(T__.Unfold().HasLabel(testLabel).Count()).
			By(T__.Unfold().HasLabel(personLabel).Count()).
		ToList()
	assert.Nil(t, err)
	assert.Equal(t,1, len(results))
	resultMap := results[0].GetInterface().(map[interface{}]interface{})
	assert.Equal(t, int64(0), resultMap[testLabel])
	assert.Equal(t, int64(len(getTestNames())), resultMap[personLabel])
}

func TestConnection(t *testing.T) {
	testHost := getenvs.GetEnvString("GREMLIN_SERVER_HOSTNAME", "localhost")
	testPort, _ := getenvs.GetEnvInt("GREMLIN_SERVER_PORT", 8182)
	runIntegration, _ := getenvs.GetEnvBool("RUN_INTEGRATION_TESTS", true)

	t.Run("Test DriverRemoteConnection GraphTraversal", func(t *testing.T) {
		if runIntegration {
			remote, err := NewDriverRemoteConnection(testHost, testPort)
			assert.Nil(t, err)
			assert.NotNil(t, remote)
			g := Traversal_().WithRemote(remote)

			// Drop the graph and check that it is empty.
			dropGraph(t, g)
			readCount(t, g, "", 0)
			readCount(t, g, testLabel, 0)
			readCount(t, g, personLabel, 0)

			// Add data and check that the size of the graph is correct.
			addTestData(t, g)
			readCount(t, g, "", len(getTestNames()))
			readCount(t, g, testLabel, 0)
			readCount(t, g, personLabel, len(getTestNames()))

			// Read test data out of the graph and check that it is correct.
			readTestDataVertexProperties(t, g)
			readTestDataValues(t, g)

			// Drop the graph and check that it is empty.
			dropGraph(t, g)
			readCount(t, g, "", 0)
			readCount(t, g, testLabel, 0)
			readCount(t, g, personLabel, 0)
		}
	})

	t.Run("Test createConnection", func(t *testing.T) {
		if runIntegration {
			connection, err := createConnection(testHost, testPort, newLogHandler(&defaultLogger{}, Info, language.English))
			assert.Nil(t, err)
			assert.NotNil(t, connection)
			err = connection.close()
			assert.Nil(t, err)
		}
	})

	t.Run("Test connection.write()", func(t *testing.T) {
		if runIntegration {
			connection, err := createConnection(testHost, testPort, newLogHandler(&defaultLogger{}, Info, language.English))
			assert.Nil(t, err)
			assert.NotNil(t, connection)
			request := makeStringRequest("g.V().count()")
			resultSet, err := connection.write(&request)
			assert.Nil(t, err)
			assert.NotNil(t, resultSet)
			result := resultSet.one()
			assert.NotNil(t, result)
			assert.Equal(t, "[0]", result.GetString())
			err = connection.close()
			assert.Nil(t, err)
		}
	})

	t.Run("Test client.submit()", func(t *testing.T) {
		if runIntegration {
			client, err := NewClient(testHost, testPort)
			assert.Nil(t, err)
			assert.NotNil(t, client)
			resultSet, err := client.Submit("g.V().count()")
			assert.Nil(t, err)
			assert.NotNil(t, resultSet)
			result := resultSet.one()
			assert.NotNil(t, result)
			assert.Equal(t, "[0]", result.GetString())
			err = client.Close()
			assert.Nil(t, err)
		}
	})

	t.Run("Test anonymousTraversal", func(t *testing.T) {
		if runIntegration {
			remote, err := NewDriverRemoteConnection(testHost, testPort)
			assert.Nil(t, err)
			assert.NotNil(t, remote)
			g := Traversal_().WithRemote(remote)

			// Drop the graph and check that it is empty.
			dropGraph(t, g)
			readCount(t, g, "", 0)
			readCount(t, g, testLabel, 0)
			readCount(t, g, personLabel, 0)

			// Add data and check that the size of the graph is correct.
			addTestData(t, g)
			readCount(t, g, "", len(getTestNames()))
			readCount(t, g, testLabel, 0)
			readCount(t, g, personLabel, len(getTestNames()))

			readUsingAnonymousTraversal(t, g)

			// Drop the graph and check that it is empty.
			dropGraph(t, g)
			readCount(t, g, "", 0)
			readCount(t, g, testLabel, 0)
			readCount(t, g, personLabel, 0)
		}
	})
}
