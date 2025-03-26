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
	"crypto/tls"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/big"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
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
const basicAuthWithSsl = "wss://localhost:45941/gremlin"

var testNames = []string{"Lyndon", "Yang", "Simon", "Rithin", "Alexey", "Valentyn"}

func newDefaultConnectionSettings() *connectionSettings {
	return &connectionSettings{
		authInfo:                 &AuthInfo{},
		tlsConfig:                &tls.Config{},
		keepAliveInterval:        keepAliveIntervalDefault,
		writeDeadline:            writeDeadlineDefault,
		connectionTimeout:        connectionTimeoutDefault,
		enableCompression:        false,
		enableUserAgentOnConnect: true,
		readBufferSize:           0,
		writeBufferSize:          0,
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

func getTestGraph(t *testing.T, url string, auth AuthInfoProvider, tls *tls.Config) *GraphTraversalSource {
	remote, err := NewDriverRemoteConnection(url,
		func(settings *DriverRemoteConnectionSettings) {
			settings.TlsConfig = tls
			settings.AuthInfo = auth
			settings.TraversalSource = testServerGraphAlias
		})
	assert.Nil(t, err)
	assert.NotNil(t, remote)
	g := Traversal_().With(remote)

	return g
}

func initializeGraph(t *testing.T, url string, auth AuthInfoProvider, tls *tls.Config) *GraphTraversalSource {
	g := getTestGraph(t, url, auth, tls)

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

func getBasicAuthInfo() *AuthInfo {
	return BasicAuthInfo(getEnvOrDefaultString("GREMLIN_GO_BASIC_AUTH_USERNAME", "stephen"),
		getEnvOrDefaultString("GREMLIN_GO_BASIC_AUTH_PASSWORD", "password"))
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
	testNoAuthAuthInfo := &AuthInfo{}
	testNoAuthTlsConfig := &tls.Config{}

	// No authentication integration test with graphs loaded and alias configured server
	testNoAuthWithAliasUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	testNoAuthWithAliasEnable := getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true)
	testNoAuthWithAliasAuthInfo := &AuthInfo{}
	testNoAuthWithAliasTlsConfig := &tls.Config{}

	// Basic authentication integration test variables.
	testBasicAuthUrl := getEnvOrDefaultString("GREMLIN_SERVER_BASIC_AUTH_URL", basicAuthWithSsl)
	testBasicAuthEnable := getEnvOrDefaultBool("RUN_BASIC_AUTH_INTEGRATION_TESTS", false)
	testBasicAuthAuthInfo := getBasicAuthInfo()
	testBasicAuthTlsConfig := &tls.Config{InsecureSkipVerify: true}

	t.Run("Test client.submit() with concurrency", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.TlsConfig = testNoAuthTlsConfig
				settings.AuthInfo = testNoAuthAuthInfo
				settings.WriteBufferSize = 1024
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
		g := initializeGraph(t, testNoAuthUrl, testNoAuthAuthInfo, testNoAuthTlsConfig)
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
		g := initializeGraph(t, testNoAuthUrl, testNoAuthAuthInfo, testNoAuthTlsConfig)
		defer g.remoteConnection.Close()

		readWithNextAndHasNext(t, g)
		resetGraph(t, g)
	})

	t.Run("Test Traversal GetResultSet", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthAuthInfo, testNoAuthTlsConfig)
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
		g := getTestGraph(t, testNoAuthUrl, testNoAuthAuthInfo, testNoAuthTlsConfig)
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
		g := initializeGraph(t, testNoAuthUrl, testNoAuthAuthInfo, testNoAuthTlsConfig)
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
		g := initializeGraph(t, testNoAuthUrl, testNoAuthAuthInfo, testNoAuthTlsConfig)
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
		g := initializeGraph(t, testNoAuthUrl, testNoAuthAuthInfo, testNoAuthTlsConfig)
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
				settings.AuthInfo = testBasicAuthAuthInfo
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
		g := initializeGraph(t, testNoAuthUrl, testNoAuthAuthInfo, testNoAuthTlsConfig)
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

	t.Run("Test DriverRemoteConnection GraphTraversal with Profile()", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthAuthInfo, testNoAuthTlsConfig)
		defer g.remoteConnection.Close()

		r, err := g.V().Has("name", "Lyndon").Values("foo").Profile().ToList()
		assert.Nil(t, err)
		assert.NotNil(t, r)
		assert.Equal(t, 1, len(r))
		metrics := r[0].Data.(*TraversalMetrics)
		assert.NotNil(t, metrics)
		assert.GreaterOrEqual(t, len(metrics.Metrics), 2)

		resetGraph(t, g)
	})

	t.Run("Test DriverRemoteConnection GraphTraversal with GremlinType", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthAuthInfo, testNoAuthTlsConfig)
		defer g.remoteConnection.Close()

		prop := &GremlinType{"java.lang.Object"}
		i := g.AddV("type_test").Property("data", prop).Iterate()
		err := <-i
		assert.Nil(t, err)

		r, err := g.V().HasLabel("type_test").Values("data").Next()
		assert.Nil(t, err)
		assert.Equal(t, prop, r.Data.(*GremlinType))

		resetGraph(t, g)
	})

	t.Run("Test DriverRemoteConnection GraphTraversal with BigDecimal", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthAuthInfo, testNoAuthTlsConfig)
		defer g.remoteConnection.Close()

		prop := &BigDecimal{11, *big.NewInt(int64(22))}
		i := g.AddV("type_test").Property("data", prop).Iterate()
		err := <-i
		assert.Nil(t, err)

		r, err := g.V().HasLabel("type_test").Values("data").Next()
		assert.Nil(t, err)
		assert.Equal(t, prop, r.Data.(*BigDecimal))

		resetGraph(t, g)
	})

	t.Run("Test DriverRemoteConnection GraphTraversal with byteBuffer", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthAuthInfo, testNoAuthTlsConfig)
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
				settings.AuthInfo = testNoAuthWithAliasAuthInfo
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

	t.Run("Test Client.Submit() Simple String Query with Bindings", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.TlsConfig = testNoAuthTlsConfig
				settings.AuthInfo = testNoAuthAuthInfo
			})
		assert.Nil(t, err)
		assert.NotNil(t, client)
		defer client.Close()

		resultSet, err := client.Submit("g.inject(x).math('_+_')", map[string]interface{}{"x": 2})
		assert.Nil(t, err)
		assert.NotNil(t, resultSet)
		result, ok, err := resultSet.One()
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.NotNil(t, result)
		res, err := result.GetInt()
		assert.Nil(t, err)
		assert.Equal(t, 4, res)
	})

	t.Run("Test Bindings To Server Configured with Modern Graph", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthWithAliasEnable)
		remote, err := NewDriverRemoteConnection(testNoAuthWithAliasUrl,
			func(settings *DriverRemoteConnectionSettings) {
				settings.TlsConfig = testNoAuthWithAliasTlsConfig
				settings.AuthInfo = testNoAuthWithAliasAuthInfo
				settings.TraversalSource = testServerModernGraphAlias
			})
		assert.Nil(t, err)
		assert.NotNil(t, remote)
		defer remote.Close()
		g := Traversal_().With(remote)

		r, err := g.V((&Bindings{}).Of("x", 1)).Out("created").Map(&Lambda{Script: "it.get().value('name').length()", Language: ""}).Sum().ToList()
		assert.Nil(t, err)
		for _, res := range r {
			assert.Equal(t, int64(3), res.GetInterface())
		}
		r, err = g.V((&Bindings{}).Of("x", 4)).Out("created").Map(&Lambda{Script: "it.get().value('name').length()", Language: ""}).Sum().ToList()
		assert.Nil(t, err)
		for _, res := range r {
			assert.Equal(t, int64(9), res.GetInterface())
		}
	})

	t.Run("Test DriverRemoteConnection Invalid GraphTraversal", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthAuthInfo, testNoAuthTlsConfig)

		// Drop the graph.
		dropGraph(t, g)

		// Add vertices and edges to graph.
		rs, err := g.AddV("person").Property("id", T__.Unfold().Property().AddV()).ToList()
		assert.Nil(t, rs)
		assert.True(t, isSameErrorCode(newError(err0502ResponseHandlerError), err))

		rs, err = g.V().Count().ToList()
		assert.NotNil(t, rs)
		assert.Nil(t, err)

		// Drop the graph.
		dropGraph(t, g)
	})

	t.Run("Test per-request arguments", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		g := getTestGraph(t, testNoAuthUrl, testNoAuthAuthInfo, testNoAuthTlsConfig)
		defer g.remoteConnection.Close()

		reqArgsTests := []struct {
			msg       string
			traversal *GraphTraversal
			nilErr    bool
		}{
			{
				"Traversal must time out (With)",
				g.
					With("evaluationTimeout", 10).
					Inject(1).
					SideEffect(&Lambda{"Thread.sleep(5000)", "gremlin-groovy"}),
				false,
			},
			{
				"Traversal must finish (With)",
				g.
					With("evaluationTimeout", 10000).
					Inject(1).
					SideEffect(&Lambda{"Thread.sleep(5000)", "gremlin-groovy"}),
				true,
			},
			{
				"evaluationTimeout is overridden and traversal must time out (With)",
				g.
					With("evaluationTimeout", 10000).With("evaluationTimeout", 10).
					Inject(1).
					SideEffect(&Lambda{"Thread.sleep(5000)", "gremlin-groovy"}),
				false,
			},
			{
				"Traversal must time out (OptionsStrategy)",
				g.
					WithStrategies(OptionsStrategy(map[string]interface{}{"evaluationTimeout": 10})).
					Inject(1).
					SideEffect(&Lambda{"Thread.sleep(5000)", "gremlin-groovy"}),
				false,
			},
			{
				"Traversal must finish (OptionsStrategy)",
				g.
					WithStrategies(OptionsStrategy(map[string]interface{}{"evaluationTimeout": 10000})).
					Inject(1).
					SideEffect(&Lambda{"Thread.sleep(5000)", "gremlin-groovy"}),
				true,
			},
		}

		gotErrs := make([]<-chan error, len(reqArgsTests))

		// Run tests in parallel.
		for i, tt := range reqArgsTests {
			gotErrs[i] = tt.traversal.Iterate()
		}

		// Check error promises.
		for i, tt := range reqArgsTests {
			assert.Equal(t, <-gotErrs[i] == nil, tt.nilErr, tt.msg)
		}
	})

	t.Run("Get all properties when materializeProperties is all", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		g := getModernGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})
		defer g.remoteConnection.Close()

		// vertex contains 2 properties, name and age
		r, err := g.With("materializeProperties", MaterializeProperties.All).V().Has("person", "name", "marko").Next()
		assert.Nil(t, err)

		AssertMarkoVertexWithProperties(t, r)
	})

	t.Run("Skip properties when materializeProperties is tokens", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		g := getModernGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})
		defer g.remoteConnection.Close()

		// vertex contains 2 properties, name and age
		r, err := g.With("materializeProperties", MaterializeProperties.Tokens).V().Has("person", "name", "marko").Next()
		assert.Nil(t, err)

		AssertMarkoVertexWithoutProperties(t, r)
	})

	t.Run("Get all properties when no materializeProperties", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		g := getModernGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})
		defer g.remoteConnection.Close()

		r, err := g.V().Has("person", "name", "marko").Next()
		assert.Nil(t, err)

		AssertMarkoVertexWithProperties(t, r)
	})

	t.Run("Test DriverRemoteConnection Traversal With materializeProperties in Modern Graph", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		g := getModernGraph(t, testNoAuthUrl, &AuthInfo{}, &tls.Config{})
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
