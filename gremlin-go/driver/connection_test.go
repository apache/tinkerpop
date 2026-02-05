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
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

const personLabel = "Person"
const testLabel = "Test"
const nameKey = "name"
const integrationTestSuiteName = "integration"
const basicAuthIntegrationTestSuite = "basic authentication integration"
const validHostInvalidPortValidPath = "http://localhost:12341253/gremlin"
const invalidHostValidPortValidPath = "http://invalidhost:8182/gremlin"
const testServerModernGraphAlias = "gmodern"
const testServerGraphAlias = "gimmutable"
const testServerCrewGraphAlias = "gcrew"
const manualTestSuiteName = "manual"
const nonRoutableIPForConnectionTimeout = "http://10.255.255.1/"

// transaction is enabled on the same port as no auth url
const noAuthUrl = "http://localhost:45940/gremlin"
const noAuthSslUrl = "https://localhost:45941/gremlin"
const basicAuthWithSsl = "https://localhost:45941/gremlin"

var testNames = []string{"Lyndon", "Yang", "Simon", "Rithin", "Alexey", "Valentyn"}

func newDefaultConnectionSettings() *connectionSettings {
	return &connectionSettings{
		tlsConfig:                &tls.Config{},
		connectionTimeout:        connectionTimeoutDefault,
		enableCompression:        false,
		enableUserAgentOnConnect: true,
	}
}

func dropGraph(t *testing.T, g *GraphTraversalSource) {
	// Drop vertices that were added.
	promise := g.V().Drop().Iterate()
	assert.NotNil(t, promise)
	assert.Nil(t, <-promise)
}

func addTestData(t *testing.T, g *GraphTraversalSource) {
	// Add vertices to traversal.
	var traversal *GraphTraversal
	for _, name := range testNames {
		if traversal == nil {
			traversal = g.AddV(personLabel).Property(nameKey, name).Property("foo", 1)
		} else {
			traversal = traversal.AddV(personLabel).Property(nameKey, name).Property("foo", 1)
		}
	}

	// Commit traversal.
	promise := traversal.Iterate()
	assert.Nil(t, <-promise)
}

func getTestGraph(t *testing.T, url string, tls *tls.Config) *GraphTraversalSource {
	remote, err := NewDriverRemoteConnection(url,
		func(settings *DriverRemoteConnectionSettings) {
			settings.TlsConfig = tls
			settings.TraversalSource = testServerGraphAlias
		})
	assert.Nil(t, err)
	assert.NotNil(t, remote)
	g := Traversal_().With(remote)

	return g
}

func initializeGraph(t *testing.T, url string, tls *tls.Config) *GraphTraversalSource {
	g := getTestGraph(t, url, tls)

	// Drop the graph and check that it is empty.
	dropGraph(t, g)
	readCount(t, g, "", 0)
	readCount(t, g, testLabel, 0)
	readCount(t, g, personLabel, 0)

	// Add data and check that the size of the graph is correct.
	addTestData(t, g)
	readCount(t, g, "", len(testNames))
	readCount(t, g, testLabel, 0)
	readCount(t, g, personLabel, len(testNames))

	return g
}

func resetGraph(t *testing.T, g *GraphTraversalSource) {
	defer func(remoteConnection *DriverRemoteConnection) {
		remoteConnection.Close()
	}(g.remoteConnection)
	// Drop the graph and check that it is empty.
	dropGraph(t, g)
	readCount(t, g, "", 0)
	readCount(t, g, testLabel, 0)
	readCount(t, g, personLabel, 0)
}

func readTestDataVertexProperties(t *testing.T, g *GraphTraversalSource) {
	// Read names from graph
	var names []string
	results, err := g.V().HasLabel(personLabel).Properties(nameKey).ToList()
	for _, result := range results {
		vp, err := result.GetVertexProperty()
		assert.Nil(t, err)
		names = append(names, vp.Value.(string))
	}
	assert.Nil(t, err)
	assert.NotNil(t, names)
	assert.True(t, sortAndCompareTwoStringSlices(names, testNames))
}

func readTestDataValues(t *testing.T, g *GraphTraversalSource) {
	// Read names from graph
	var names []string
	results, err := g.V().HasLabel(personLabel).Values(nameKey).ToList()
	for _, result := range results {
		names = append(names, result.GetString())
	}
	assert.Nil(t, err)
	assert.NotNil(t, names)
	assert.True(t, sortAndCompareTwoStringSlices(names, testNames))
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

func sortAndCompareTwoStringSlices(s1 []string, s2 []string) bool {
	sort.Strings(s1)
	sort.Strings(s2)
	return reflect.DeepEqual(s1, s2)
}

func readUsingAnonymousTraversal(t *testing.T, g *GraphTraversalSource) {
	results, err := g.V().Fold().
		Project(testLabel, personLabel).
		By(T__.Unfold().HasLabel(testLabel).Count()).
		By(T__.Unfold().HasLabel(personLabel).Count()).
		ToList()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	resultMap := results[0].GetInterface().(map[interface{}]interface{})
	assert.Equal(t, int64(0), resultMap[testLabel])
	assert.Equal(t, int64(len(testNames)), resultMap[personLabel])
}

func readWithNextAndHasNext(t *testing.T, g *GraphTraversalSource) {
	traversal := g.V().HasLabel(personLabel).Properties(nameKey)
	var names []string
	for i := 0; i < len(testNames); i++ {
		hasN, err := traversal.HasNext()
		assert.Nil(t, err)
		assert.True(t, hasN)
		res, err := traversal.Next()
		assert.Nil(t, err)
		assert.NotNil(t, res)
		vp, err := res.GetVertexProperty()
		assert.Nil(t, err)
		names = append(names, vp.Value.(string))
	}
	hasN, _ := traversal.HasNext()
	assert.False(t, hasN)
	// Check for Next error when no more elements left
	res, err := traversal.Next()
	assert.Nil(t, res)
	assert.Equal(t, newError(err0903NextNoResultsLeftError), err)
	assert.True(t, sortAndCompareTwoStringSlices(names, testNames))
}

func getEnvOrDefaultString(key string, defaultValue string) string {
	// Missing value is returned as "".
	value := os.Getenv(key)
	if len(value) != 0 {
		return value
	}
	return defaultValue
}

func getEnvOrDefaultBool(key string, defaultValue bool) bool {
	value := getEnvOrDefaultString(key, "")
	if len(value) != 0 {
		boolValue, err := strconv.ParseBool(value)
		if err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func skipTestsIfNotEnabled(t *testing.T, testSuiteName string, testSuiteEnabled bool) {
	if !testSuiteEnabled {
		t.Skipf("Skipping %s because %s tests are not enabled.", t.Name(), testSuiteName)
	}
}

func TestConnection(t *testing.T) {
	// Integration test variables.
	testNoAuthUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	testNoAuthEnable := getEnvOrDefaultBool("RUN_INTEGRATION_TESTS", true)
	testNoAuthTlsConfig := &tls.Config{}

	// No authentication integration test with graphs loaded and alias configured server
	testNoAuthWithAliasUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	testNoAuthWithAliasEnable := getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true)
	testNoAuthWithAliasTlsConfig := &tls.Config{}

	// Basic authentication integration test variables.
	testBasicAuthUrl := getEnvOrDefaultString("GREMLIN_SERVER_BASIC_AUTH_URL", basicAuthWithSsl)
	testBasicAuthEnable := getEnvOrDefaultBool("RUN_BASIC_AUTH_INTEGRATION_TESTS", false)
	testBasicAuthUsername := getEnvOrDefaultString("GREMLIN_GO_BASIC_AUTH_USERNAME", "stephen")
	testBasicAuthPassword := getEnvOrDefaultString("GREMLIN_GO_BASIC_AUTH_PASSWORD", "password")
	testBasicAuthTlsConfig := &tls.Config{InsecureSkipVerify: true}

	// this test is used to test the ws->http POC changes via manual execution with a local TP 4.0 gremlin server running on 8182
	t.Run("Test client.submit()", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		tlsConf := tls.Config{
			InsecureSkipVerify: true,
		}

		client, err := NewClient(testNoAuthUrl,
			//client, err := NewClient(noAuthSslUrl,
			func(settings *ClientSettings) {
				settings.TlsConfig = &tlsConf
				settings.EnableCompression = true
				settings.TraversalSource = testServerModernGraphAlias
			})
		assert.Nil(t, err)
		assert.NotNil(t, client)
		defer client.Close()

		// synchronous
		for i := 0; i < 5; i++ {
			submitCount(i, client, t)
		}

		// async
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				submitCount(i, client, t)
			}(i)
		}
		wg.Wait()
	})

	t.Run("Test client.submit() with concurrency", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.TlsConfig = testNoAuthTlsConfig
				settings.EnableCompression = true
				settings.TraversalSource = testServerModernGraphAlias
			})
		assert.Nil(t, err)
		assert.NotNil(t, client)
		defer client.Close()

		// synchronous
		for i := 0; i < 5; i++ {
			submitCount(i, client, t)
		}

		// async
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				submitCount(i, client, t)
			}(i)
		}
		wg.Wait()
	})

	t.Run("Test DriverRemoteConnection GraphTraversal", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthTlsConfig)
		defer g.remoteConnection.Close()

		// Read test data out of the graph and check that it is correct.
		readTestDataVertexProperties(t, g)
		readTestDataValues(t, g)

		// Drop the graph and check that it is empty.
		resetGraph(t, g)
	})

	t.Run("Test Traversal. Next and HasNext", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthTlsConfig)
		defer g.remoteConnection.Close()

		readWithNextAndHasNext(t, g)
		resetGraph(t, g)
	})

	t.Run("Test Traversal GetResultSet", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthTlsConfig)
		defer g.remoteConnection.Close()

		resultSet, err := g.V().HasLabel(personLabel).Properties(nameKey).GetResultSet()
		assert.Nil(t, err)
		assert.NotNil(t, resultSet)
		allResults, err := resultSet.All()
		assert.Nil(t, err)
		var names []string
		for _, res := range allResults {
			assert.NotNil(t, res)
			vp, err := res.GetVertexProperty()
			assert.Nil(t, err)
			names = append(names, vp.Value.(string))
		}
		assert.True(t, sortAndCompareTwoStringSlices(names, testNames))

		resetGraph(t, g)
	})

	t.Run("Test DriverRemoteConnection GraphTraversal With Label", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := getTestGraph(t, testNoAuthUrl, testNoAuthTlsConfig)
		defer g.remoteConnection.Close()

		// Drop the graph.
		dropGraph(t, g)

		// Add vertices and edges to graph.
		i := g.AddV("company").
			Property("name", "Bit-Quill").As("bq").
			AddV("software").
			Property("name", "GremlinServer").As("gs").
			AddV("software").
			Property("name", "TinkerPop").As("tp").
			AddE("WORKS_ON").From("bq").To("tp").
			AddE("IS_IN").From("gs").To("tp").
			AddE("LIKES").From("bq").To("tp").Iterate()
		assert.Nil(t, <-i)

		results, errs := g.V().OutE().InV().Path().By("name").By(T.Label).ToList()
		assert.Nil(t, errs)
		assert.NotNil(t, results)
		assert.Equal(t, 3, len(results))

		possiblePaths := []string{"path[Bit-Quill, WORKS_ON, TinkerPop]", "path[Bit-Quill, LIKES, TinkerPop]", "path[GremlinServer, IS_IN, TinkerPop]"}
		for _, result := range results {
			found := false
			for _, path := range possiblePaths {
				p, err := result.GetPath()
				assert.Nil(t, err)
				if path == p.String() {
					found = true
					break
				}
			}
			assert.True(t, found)
		}

		// Drop the graph.
		dropGraph(t, g)
	})

	t.Run("Test DriverRemoteConnection GraphTraversal P", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthTlsConfig)
		defer g.remoteConnection.Close()

		// Read test data out of the graph and check that it is correct.
		results, err := g.V().Has("name", P.Eq("Lyndon")).ValueMap("name").ToList()
		assert.Nil(t, err)
		assert.Equal(t, 1, len(results))

		// Drop the graph and check that it is empty.
		resetGraph(t, g)
	})

	t.Run("Test DriverRemoteConnection Next and HasNext", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthTlsConfig)
		defer g.remoteConnection.Close()

		// Run traversal and test Next/HasNext calls
		traversal := g.V().HasLabel(personLabel).Properties(nameKey)
		var names []string
		for i := 0; i < len(testNames); i++ {
			hasN, err := traversal.HasNext()
			assert.Nil(t, err)
			assert.True(t, hasN)
			res, err := traversal.Next()
			assert.Nil(t, err)
			assert.NotNil(t, res)
			vp, err := res.GetVertexProperty()
			assert.Nil(t, err)
			names = append(names, vp.Value.(string))
		}
		hasN, _ := traversal.HasNext()
		assert.False(t, hasN)
		assert.True(t, sortAndCompareTwoStringSlices(names, testNames))
	})

	t.Run("Test anonymousTraversal", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthTlsConfig)
		defer g.remoteConnection.Close()

		readUsingAnonymousTraversal(t, g)

		// Drop the graph and check that it is empty.
		resetGraph(t, g)
	})

	t.Run("Test Traversal.ToList fail", func(t *testing.T) {
		anonTrav := T__.Unfold().HasLabel(testLabel)
		slice, err := anonTrav.ToList()
		assert.Nil(t, slice)
		assert.Equal(t, newError(err0901ToListAnonTraversalError), err)
	})

	t.Run("Test Traversal.Iterate fail", func(t *testing.T) {
		anonTrav := T__.Unfold().HasLabel(testLabel)
		channel := anonTrav.Iterate()
		assert.NotNil(t, channel)
		err := <-channel
		assert.Equal(t, newError(err0902IterateAnonTraversalError), err)
	})

	t.Run("Test DriverRemoteConnection with basic authentication", func(t *testing.T) {
		skipTestsIfNotEnabled(t, basicAuthIntegrationTestSuite, testBasicAuthEnable)
		remote, err := NewDriverRemoteConnection(testBasicAuthUrl,
			func(settings *DriverRemoteConnectionSettings) {
				settings.TlsConfig = testBasicAuthTlsConfig
				settings.RequestInterceptors = []RequestInterceptor{
					BasicAuth(testBasicAuthUsername, testBasicAuthPassword),
				}
			})
		assert.Nil(t, err)
		assert.NotNil(t, remote)
		// Close remote connection.
		defer remote.Close()

		g := Traversal_().With(remote)

		// Drop the graph and check that it is empty.
		dropGraph(t, g)

		// Check that graph is empty.
		count, err := g.V().Count().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, count)
		assert.Equal(t, 1, len(count))
		val, err := count[0].GetInt32()
		assert.Nil(t, err)
		assert.Equal(t, int32(0), val)
	})

	t.Run("Test DriverRemoteConnection GraphTraversal WithSack", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthTlsConfig)
		defer g.remoteConnection.Close()

		r, err := g.WithSack(1).V().Has("name", "Lyndon").Values("foo").Sack(Operator.Sum).Sack().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, r)
		assert.Equal(t, 1, len(r))
		val, err := r[0].GetInt32()
		assert.Nil(t, err)
		assert.Equal(t, int32(2), val)

		resetGraph(t, g)
	})

	// TODO re-enable after profile() step is updated in server
	//t.Run("Test DriverRemoteConnection GraphTraversal with Profile()", func(t *testing.T) {
	//	skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
	//
	//	// Initialize graph
	//	g := initializeGraph(t, testNoAuthUrl, testNoAuthTlsConfig)
	//	defer g.remoteConnection.Close()
	//
	//	r, err := g.V().Has("name", "Lyndon").Values("foo").Profile().ToList()
	//	assert.Nil(t, err)
	//	assert.NotNil(t, r)
	//	assert.Equal(t, 1, len(r))
	//	metrics := r[0].Data.(*TraversalMetrics)
	//	assert.NotNil(t, metrics)
	//	assert.GreaterOrEqual(t, len(metrics.Metrics), 2)
	//
	//	resetGraph(t, g)
	//})

	t.Run("Test DriverRemoteConnection GraphTraversal with BigDecimal", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthTlsConfig)
		defer g.remoteConnection.Close()

		prop := &BigDecimal{11, big.NewInt(int64(22))}
		i := g.AddV("type_test").Property("data", prop).Iterate()
		err := <-i
		assert.Nil(t, err)

		// TODO revisit BigDecimal implementation
		//r, err := g.V().HasLabel("type_test").Values("data").Next()
		//assert.Nil(t, err)
		//assert.Equal(t, prop, r.Data.(*BigDecimal))

		resetGraph(t, g)
	})

	t.Run("Test DriverRemoteConnection GraphTraversal with byteBuffer", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthTlsConfig)
		defer g.remoteConnection.Close()

		prop := &ByteBuffer{[]byte{byte(127), byte(255)}}
		i := g.AddV("type_test").Property("data", prop).Iterate()
		err := <-i
		assert.Nil(t, err)

		r, err := g.V().HasLabel("type_test").Values("data").Next()
		assert.Nil(t, err)
		assert.Equal(t, prop, r.Data)

		resetGraph(t, g)
	})

	t.Run("Test DriverRemoteConnection To Server Configured with Modern Graph", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthWithAliasEnable)
		remote, err := NewDriverRemoteConnection(testNoAuthWithAliasUrl,
			func(settings *DriverRemoteConnectionSettings) {
				settings.TlsConfig = testNoAuthWithAliasTlsConfig
				settings.TraversalSource = testServerModernGraphAlias
			})
		assert.Nil(t, err)
		assert.NotNil(t, remote)
		defer remote.Close()

		g := Traversal_().With(remote)

		r, err := g.V().Count().ToList()
		assert.Nil(t, err)
		for _, res := range r {
			assert.Equal(t, int64(6), res.GetInterface())
		}
	})

	t.Run("Test DriverRemoteConnection Invalid GraphTraversal", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthTlsConfig)

		// Drop the graph.
		dropGraph(t, g)

		// Add vertices and edges to graph.
		rs, err := g.AddV("person").Property("id", T__.Unfold().Property().AddV()).ToList()
		assert.Nil(t, rs)
		assert.True(t, isSameErrorCode(newError(err0502ResponseError), err))

		rs, err = g.V().Count().ToList()
		assert.NotNil(t, rs)
		assert.Nil(t, err)

		// Drop the graph.
		dropGraph(t, g)
	})

	t.Run("Get all properties when materializeProperties is all", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		g := getModernGraph(t, testNoAuthUrl, &tls.Config{})
		defer g.remoteConnection.Close()

		// vertex contains 2 properties, name and age
		r, err := g.With("materializeProperties", MaterializeProperties.All).V().Has("person", "name", "marko").Next()
		assert.Nil(t, err)

		AssertMarkoVertexWithProperties(t, r)
	})

	t.Run("Skip properties when materializeProperties is tokens", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		g := getModernGraph(t, testNoAuthUrl, &tls.Config{})
		defer g.remoteConnection.Close()

		// vertex contains 2 properties, name and age
		r, err := g.With("materializeProperties", MaterializeProperties.Tokens).V().Has("person", "name", "marko").Next()
		assert.Nil(t, err)

		AssertMarkoVertexWithoutProperties(t, r)
	})

	t.Run("Get all properties when no materializeProperties", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		g := getModernGraph(t, testNoAuthUrl, &tls.Config{})
		defer g.remoteConnection.Close()

		r, err := g.V().Has("person", "name", "marko").Next()
		assert.Nil(t, err)

		AssertMarkoVertexWithProperties(t, r)
	})

	t.Run("Test DriverRemoteConnection Traversal With materializeProperties in Modern Graph", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		g := getModernGraph(t, testNoAuthUrl, &tls.Config{})
		defer g.remoteConnection.Close()

		vertices, err := g.With("materializeProperties", MaterializeProperties.Tokens).V().ToList()
		assert.Nil(t, err)
		for _, res := range vertices {
			v, _ := res.GetVertex()
			assert.Nil(t, err)
			properties, ok := v.Properties.([]interface{})
			assert.True(t, ok)
			assert.Equal(t, 0, len(properties))
		}

		edges, err := g.With("materializeProperties", MaterializeProperties.Tokens).E().ToList()
		assert.Nil(t, err)
		for _, res := range edges {
			e, _ := res.GetEdge()
			assert.Nil(t, err)
			properties, ok := e.Properties.([]interface{})
			assert.True(t, ok)
			assert.Equal(t, 0, len(properties))
		}

		vps, err := g.With("materializeProperties", MaterializeProperties.Tokens).V().Properties().ToList()
		assert.Nil(t, err)
		for _, res := range vps {
			vp, _ := res.GetVertexProperty()
			assert.Nil(t, err)
			properties, ok := vp.Properties.([]interface{})
			assert.True(t, ok)
			assert.Equal(t, 0, len(properties))
		}

		// Path elements should also have no materialized properties when tokens is set
		r, err := g.With("materializeProperties", MaterializeProperties.Tokens).V().Has("person", "name", "marko").OutE().InV().HasLabel("software").Path().Next()
		assert.Nil(t, err)
		p, err := r.GetPath()
		assert.Nil(t, err)
		assert.NotNil(t, p)
		assert.Equal(t, 3, len(p.Objects))

		// first element should be a Vertex
		if a, ok := p.Objects[0].(*Vertex); assert.True(t, ok) {
			props, ok := a.Properties.([]interface{})
			assert.True(t, ok)
			assert.Equal(t, 0, len(props))
		}
		// second element should be an Edge
		if b, ok := p.Objects[1].(*Edge); assert.True(t, ok) {
			props, ok := b.Properties.([]interface{})
			assert.True(t, ok)
			assert.Equal(t, 0, len(props))
		}
		// third element should be a Vertex
		if c, ok := p.Objects[2].(*Vertex); assert.True(t, ok) {
			props, ok := c.Properties.([]interface{})
			assert.True(t, ok)
			assert.Equal(t, 0, len(props))
		}

		// Path elements should have materialized properties when all is set
		r, err = g.With("materializeProperties", MaterializeProperties.All).V().Has("person", "name", "marko").OutE().InV().HasLabel("software").Path().Next()
		assert.Nil(t, err)
		p, err = r.GetPath()
		assert.Nil(t, err)
		assert.NotNil(t, p)
		assert.Equal(t, 3, len(p.Objects))

		// first element should be a Vertex with properties present
		if a, ok := p.Objects[0].(*Vertex); assert.True(t, ok) {
			props, ok := a.Properties.([]interface{})
			assert.True(t, ok)
			assert.Greater(t, len(props), 0)
		}
		// second element should be an Edge with properties present
		if b, ok := p.Objects[1].(*Edge); assert.True(t, ok) {
			props, ok := b.Properties.([]interface{})
			assert.True(t, ok)
			assert.Greater(t, len(props), 0)
		}
		// third element should be a Vertex with properties present
		if c, ok := p.Objects[2].(*Vertex); assert.True(t, ok) {
			props, ok := c.Properties.([]interface{})
			assert.True(t, ok)
			assert.Greater(t, len(props), 0)
		}
	})
}

func submitCount(i int, client *Client, t *testing.T) {
	resultSet, err := client.Submit("g.V().count().as('c').math('c + " + strconv.Itoa(i) + "')")
	assert.Nil(t, err)
	assert.NotNil(t, resultSet)
	result, ok, err := resultSet.One()
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.NotNil(t, result)
	c, err := result.GetInt()
	assert.Equal(t, 6+i, c)
	_, _ = fmt.Fprintf(os.Stdout, "Received result : %s\n", result)
}

func TestStreamingResultDelivery(t *testing.T) {
	testNoAuthWithAliasEnable := getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true)
	skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthWithAliasEnable)
	remote, err := NewDriverRemoteConnection(getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl),
		func(settings *DriverRemoteConnectionSettings) {
			settings.TlsConfig = &tls.Config{}
			settings.TraversalSource = "ggrateful"
		})
	assert.Nil(t, err)
	assert.NotNil(t, remote)
	g := Traversal_().With(remote)
	defer g.remoteConnection.Close()

	t.Run("first result arrives before all results", func(t *testing.T) {
		start := time.Now()
		rs, err := g.V().Properties().GetResultSet()
		assert.Nil(t, err)

		// First result should arrive quickly
		_, ok, err := rs.One()
		firstResultTime := time.Since(start)
		assert.Nil(t, err)
		assert.True(t, ok)

		// Drain remaining
		_, err = rs.All()
		assert.Nil(t, err)
		totalTime := time.Since(start)

		t.Logf("First result: %v, Total: %v, Ratio: %.2f%%",
			firstResultTime, totalTime, float64(firstResultTime)/float64(totalTime)*100)
	})

	t.Run("results arrive incrementally", func(t *testing.T) {
		rs, err := g.V().Properties().GetResultSet()
		assert.Nil(t, err)

		var timestamps []time.Time
		start := time.Now()

		for {
			_, ok, err := rs.One()
			assert.Nil(t, err)
			if !ok {
				break
			}
			timestamps = append(timestamps, time.Now())
		}

		if len(timestamps) < 2 {
			t.Skip("need more results to test incremental delivery")
		}

		firstHalf := timestamps[len(timestamps)/2].Sub(start)
		total := timestamps[len(timestamps)-1].Sub(start)
		t.Logf("Half results at: %v, All results at: %v", firstHalf, total)
	})
}

// Unit tests for connection (moved from httpConnection_test.go)

func newTestLogHandler() *logHandler {
	return newLogHandler(&defaultLogger{}, Warning, language.English)
}

func TestNewConnection(t *testing.T) {
	t.Run("creates connection with default settings", func(t *testing.T) {
		conn := newConnection(newTestLogHandler(), "http://localhost:8182/gremlin", &connectionSettings{})

		assert.NotNil(t, conn.httpClient)
		assert.NotNil(t, conn.httpClient.Transport)
	})

	t.Run("applies TLS config", func(t *testing.T) {
		tlsConfig := &tls.Config{InsecureSkipVerify: true}
		conn := newConnection(newTestLogHandler(), "https://localhost:8182/gremlin", &connectionSettings{
			tlsConfig: tlsConfig,
		})

		transport := conn.httpClient.Transport.(*http.Transport)
		assert.Equal(t, tlsConfig, transport.TLSClientConfig)
	})
}

func TestSetHttpRequestHeaders(t *testing.T) {
	t.Run("sets content type and accept headers", func(t *testing.T) {
		conn := newConnection(newTestLogHandler(), "http://localhost/gremlin", &connectionSettings{})
		req, _ := NewHttpRequest(http.MethodPost, "http://localhost/gremlin")

		conn.setHttpRequestHeaders(req)

		assert.Equal(t, graphBinaryMimeType, req.Headers.Get("Content-Type"))
		assert.Equal(t, graphBinaryMimeType, req.Headers.Get("Accept"))
	})

	t.Run("sets user agent when enabled", func(t *testing.T) {
		conn := newConnection(newTestLogHandler(), "http://localhost/gremlin", &connectionSettings{
			enableUserAgentOnConnect: true,
		})
		req, _ := NewHttpRequest(http.MethodPost, "http://localhost/gremlin")

		conn.setHttpRequestHeaders(req)

		assert.NotEmpty(t, req.Headers.Get(HeaderUserAgent))
	})

	t.Run("sets compression header when enabled", func(t *testing.T) {
		conn := newConnection(newTestLogHandler(), "http://localhost/gremlin", &connectionSettings{
			enableCompression: true,
		})
		req, _ := NewHttpRequest(http.MethodPost, "http://localhost/gremlin")

		conn.setHttpRequestHeaders(req)

		assert.Equal(t, "deflate", req.Headers.Get("Accept-Encoding"))
	})
}

func TestGetReader(t *testing.T) {
	conn := newConnection(newTestLogHandler(), "http://localhost/gremlin", &connectionSettings{})

	t.Run("returns body for non-compressed response", func(t *testing.T) {
		resp := &http.Response{
			Header: http.Header{},
			Body:   http.NoBody,
		}

		reader, closer, err := conn.getReader(resp)

		assert.NoError(t, err)
		assert.Nil(t, closer)
		assert.Equal(t, resp.Body, reader)
	})

	t.Run("returns zlib reader for deflate response", func(t *testing.T) {
		// Valid zlib compressed empty data
		zlibData := []byte{0x78, 0x9c, 0x03, 0x00, 0x00, 0x00, 0x00, 0x01}
		resp := &http.Response{
			Header: http.Header{"Content-Encoding": []string{"deflate"}},
			Body:   io.NopCloser(bytes.NewReader(zlibData)),
		}

		reader, closer, err := conn.getReader(resp)

		assert.NoError(t, err)
		assert.NotNil(t, closer)
		assert.NotNil(t, reader)
		require.NoError(t, closer.Close())
	})

	t.Run("returns error for invalid zlib data", func(t *testing.T) {
		resp := &http.Response{
			Header: http.Header{"Content-Encoding": []string{"deflate"}},
			Body:   io.NopCloser(bytes.NewReader([]byte{0x00, 0x00})),
		}

		_, _, err := conn.getReader(resp)

		assert.Error(t, err)
	})
}

func TestConnectionWithMockServer(t *testing.T) {
	t.Run("handles connection error", func(t *testing.T) {
		conn := newConnection(newTestLogHandler(), "http://localhost:99999/gremlin", &connectionSettings{
			connectionTimeout: 100 * time.Millisecond,
		})

		rs, err := conn.submit(&request{gremlin: "g.V()", fields: map[string]interface{}{}})
		assert.NoError(t, err) // submit returns nil, error goes to ResultSet

		// All() blocks until stream closes, then we can check error
		_, _ = rs.All()
		assert.Error(t, rs.GetError())
	})

	t.Run("receives headers from request", func(t *testing.T) {
		headersCh := make(chan http.Header, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			headersCh <- r.Header.Clone()
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		conn := newConnection(newTestLogHandler(), server.URL, &connectionSettings{
			enableUserAgentOnConnect: true,
			enableCompression:        true,
		})

		rs, err := conn.submit(&request{gremlin: "g.V()", fields: map[string]interface{}{}})
		require.NoError(t, err)

		select {
		case receivedHeaders := <-headersCh:
			assert.Equal(t, graphBinaryMimeType, receivedHeaders.Get("Content-Type"))
			assert.Equal(t, "deflate", receivedHeaders.Get("Accept-Encoding"))
			assert.NotEmpty(t, receivedHeaders.Get(userAgentHeader))
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for request")
		}

		_, _ = rs.All() // drain
	})
}

// Tests for connection pool configuration settings

func TestConnectionPoolSettings(t *testing.T) {
	t.Run("default values are applied when settings are 0", func(t *testing.T) {
		// Create connection with empty settings (all zeros)
		conn := newConnection(newTestLogHandler(), "http://localhost:8182/gremlin", &connectionSettings{})

		transport := conn.httpClient.Transport.(*http.Transport)

		// Verify default values are applied
		assert.Equal(t, 128, transport.MaxConnsPerHost, "MaxConnsPerHost should default to 128")
		assert.Equal(t, 8, transport.MaxIdleConnsPerHost, "MaxIdleConnsPerHost should default to 8")
		assert.Equal(t, 180*time.Second, transport.IdleConnTimeout, "IdleConnTimeout should default to 180s")
	})

	t.Run("custom values are passed through to http.Transport", func(t *testing.T) {
		customSettings := &connectionSettings{
			maxConnsPerHost:     256,
			maxIdleConnsPerHost: 16,
			idleConnTimeout:     300 * time.Second,
			keepAliveInterval:   60 * time.Second,
			connectionTimeout:   30 * time.Second,
		}

		conn := newConnection(newTestLogHandler(), "http://localhost:8182/gremlin", customSettings)

		transport := conn.httpClient.Transport.(*http.Transport)

		// Verify custom values are applied
		assert.Equal(t, 256, transport.MaxConnsPerHost, "MaxConnsPerHost should be custom value")
		assert.Equal(t, 16, transport.MaxIdleConnsPerHost, "MaxIdleConnsPerHost should be custom value")
		assert.Equal(t, 300*time.Second, transport.IdleConnTimeout, "IdleConnTimeout should be custom value")
	})

	t.Run("partial custom settings use defaults for unset values", func(t *testing.T) {
		// Only set maxConnsPerHost, leave others at 0
		customSettings := &connectionSettings{
			maxConnsPerHost: 64,
		}

		conn := newConnection(newTestLogHandler(), "http://localhost:8182/gremlin", customSettings)

		transport := conn.httpClient.Transport.(*http.Transport)

		// Verify custom value is applied
		assert.Equal(t, 64, transport.MaxConnsPerHost, "MaxConnsPerHost should be custom value")
		// Verify defaults are used for unset values
		assert.Equal(t, 8, transport.MaxIdleConnsPerHost, "MaxIdleConnsPerHost should default to 8")
		assert.Equal(t, 180*time.Second, transport.IdleConnTimeout, "IdleConnTimeout should default to 180s")
	})
}

func TestClientSettingsWiring(t *testing.T) {
	t.Run("ClientSettings wires connection pool settings", func(t *testing.T) {
		client, err := NewClient("http://localhost:8182/gremlin",
			func(settings *ClientSettings) {
				settings.MaximumConcurrentConnections = 200
				settings.MaxIdleConnections = 20
				settings.IdleConnectionTimeout = 240 * time.Second
				settings.KeepAliveInterval = 45 * time.Second
			})
		require.NoError(t, err)
		defer client.Close()

		// Verify settings were wired to connectionSettings
		assert.Equal(t, 200, client.connectionSettings.maxConnsPerHost)
		assert.Equal(t, 20, client.connectionSettings.maxIdleConnsPerHost)
		assert.Equal(t, 240*time.Second, client.connectionSettings.idleConnTimeout)
		assert.Equal(t, 45*time.Second, client.connectionSettings.keepAliveInterval)

		// Verify settings were applied to http.Transport
		transport := client.conn.httpClient.Transport.(*http.Transport)
		assert.Equal(t, 200, transport.MaxConnsPerHost)
		assert.Equal(t, 20, transport.MaxIdleConnsPerHost)
		assert.Equal(t, 240*time.Second, transport.IdleConnTimeout)
	})

	t.Run("ClientSettings uses defaults when not configured", func(t *testing.T) {
		client, err := NewClient("http://localhost:8182/gremlin")
		require.NoError(t, err)
		defer client.Close()

		// Verify defaults are used (0 in settings means use default)
		assert.Equal(t, 0, client.connectionSettings.maxConnsPerHost)
		assert.Equal(t, 0, client.connectionSettings.maxIdleConnsPerHost)
		assert.Equal(t, time.Duration(0), client.connectionSettings.idleConnTimeout)
		assert.Equal(t, time.Duration(0), client.connectionSettings.keepAliveInterval)

		// Verify defaults were applied to http.Transport
		transport := client.conn.httpClient.Transport.(*http.Transport)
		assert.Equal(t, 128, transport.MaxConnsPerHost)
		assert.Equal(t, 8, transport.MaxIdleConnsPerHost)
		assert.Equal(t, 180*time.Second, transport.IdleConnTimeout)
	})
}

func TestDriverRemoteConnectionSettingsWiring(t *testing.T) {
	t.Run("DriverRemoteConnectionSettings wires connection pool settings", func(t *testing.T) {
		drc, err := NewDriverRemoteConnection("http://localhost:8182/gremlin",
			func(settings *DriverRemoteConnectionSettings) {
				settings.MaximumConcurrentConnections = 150
				settings.MaxIdleConnections = 15
				settings.IdleConnectionTimeout = 200 * time.Second
				settings.KeepAliveInterval = 40 * time.Second
			})
		require.NoError(t, err)
		defer drc.Close()

		// Verify settings were wired to connectionSettings
		assert.Equal(t, 150, drc.client.connectionSettings.maxConnsPerHost)
		assert.Equal(t, 15, drc.client.connectionSettings.maxIdleConnsPerHost)
		assert.Equal(t, 200*time.Second, drc.client.connectionSettings.idleConnTimeout)
		assert.Equal(t, 40*time.Second, drc.client.connectionSettings.keepAliveInterval)

		// Verify settings were applied to http.Transport
		transport := drc.client.conn.httpClient.Transport.(*http.Transport)
		assert.Equal(t, 150, transport.MaxConnsPerHost)
		assert.Equal(t, 15, transport.MaxIdleConnsPerHost)
		assert.Equal(t, 200*time.Second, transport.IdleConnTimeout)
	})

	t.Run("DriverRemoteConnectionSettings uses defaults when not configured", func(t *testing.T) {
		drc, err := NewDriverRemoteConnection("http://localhost:8182/gremlin")
		require.NoError(t, err)
		defer drc.Close()

		// Verify defaults are used (0 in settings means use default)
		assert.Equal(t, 0, drc.client.connectionSettings.maxConnsPerHost)
		assert.Equal(t, 0, drc.client.connectionSettings.maxIdleConnsPerHost)
		assert.Equal(t, time.Duration(0), drc.client.connectionSettings.idleConnTimeout)
		assert.Equal(t, time.Duration(0), drc.client.connectionSettings.keepAliveInterval)

		// Verify defaults were applied to http.Transport
		transport := drc.client.conn.httpClient.Transport.(*http.Transport)
		assert.Equal(t, 128, transport.MaxConnsPerHost)
		assert.Equal(t, 8, transport.MaxIdleConnsPerHost)
		assert.Equal(t, 180*time.Second, transport.IdleConnTimeout)
	})
}
