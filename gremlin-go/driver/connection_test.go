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
	"math/big"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

const personLabel = "Person"
const testLabel = "Test"
const nameKey = "name"
const integrationTestSuiteName = "integration"
const basicAuthIntegrationTestSuite = "basic authentication integration"
const validHostInvalidPortValidPath = "ws://localhost:12341253/gremlin"
const invalidHostValidPortValidPath = "ws://invalidhost:8182/gremlin"
const testServerModernGraphAlias = "gmodern"
const testServerGraphAlias = "gimmutable"
const manualTestSuiteName = "manual"
const nonRoutableIPForConnectionTimeout = "ws://10.255.255.1/"

// transaction is enabled on the same port as no auth url
const noAuthUrl = "ws://localhost:45940/gremlin"
const basicAuthWithSsl = "wss://localhost:45941/gremlin"

var testNames = []string{"Lyndon", "Yang", "Simon", "Rithin", "Alexey", "Valentyn"}

func newDefaultConnectionSettings() *connectionSettings {
	return &connectionSettings{
		authInfo:          &AuthInfo{},
		tlsConfig:         &tls.Config{},
		keepAliveInterval: keepAliveIntervalDefault,
		writeDeadline:     writeDeadlineDefault,
		connectionTimeout: connectionTimeoutDefault,
		enableCompression: false,
		readBufferSize:    0,
		writeBufferSize:   0,
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

func getTestGraph(t *testing.T, url string, auth *AuthInfo, tls *tls.Config) *GraphTraversalSource {
	remote, err := NewDriverRemoteConnection(url,
		func(settings *DriverRemoteConnectionSettings) {
			settings.TlsConfig = tls
			settings.AuthInfo = auth
			settings.TraversalSource = testServerGraphAlias
		})
	assert.Nil(t, err)
	assert.NotNil(t, remote)
	g := Traversal_().WithRemote(remote)

	return g
}

func initializeGraph(t *testing.T, url string, auth *AuthInfo, tls *tls.Config) *GraphTraversalSource {
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

func deferredCleanup(t *testing.T, connection *connection) {
	assert.Nil(t, connection.close())
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
	// TODO using "wss:" to connect to an auth server without ssl enabled ("ws:") will give an "EOF" error in NewDriverRemoteConnection, bug?
	testBasicAuthUrl := getEnvOrDefaultString("GREMLIN_SERVER_BASIC_AUTH_URL", basicAuthWithSsl)
	testBasicAuthEnable := getEnvOrDefaultBool("RUN_BASIC_AUTH_INTEGRATION_TESTS", false)
	testBasicAuthAuthInfo := getBasicAuthInfo()
	testBasicAuthTlsConfig := &tls.Config{InsecureSkipVerify: true}

	testManual := getEnvOrDefaultBool("RUN_MANUAL_TEST", false)

	t.Run("Test createConnection without valid server", func(t *testing.T) {
		connection, err := createConnection(invalidHostValidPortValidPath, newLogHandler(&defaultLogger{}, Info,
			language.English), newDefaultConnectionSettings())
		assert.NotNil(t, err)
		assert.Nil(t, connection)
	})

	t.Run("Test createConnection without valid port", func(t *testing.T) {
		connection, err := createConnection(validHostInvalidPortValidPath, newLogHandler(&defaultLogger{}, Info,
			language.English), newDefaultConnectionSettings())
		assert.NotNil(t, err)
		assert.Nil(t, connection)
	})

	t.Run("Test createConnection without valid path", func(t *testing.T) {
		connSettings := newDefaultConnectionSettings()
		t.Run("Test 1 second timeout", func(t *testing.T) {
			connSettings.connectionTimeout = 1 * time.Second
			t1 := time.Now()
			connection, err := createConnection(nonRoutableIPForConnectionTimeout, newLogHandler(&defaultLogger{}, Info,
				language.English), connSettings)
			t2 := time.Since(t1)
			assert.True(t, t2.Seconds() < 1.5 && t2.Seconds() > 0.5)
			assert.NotNil(t, err)
			assert.Nil(t, connection)
		})

		t.Run("Test 2 second timeout", func(t *testing.T) {
			connSettings.connectionTimeout = 2 * time.Second
			t1 := time.Now()
			connection, err := createConnection(nonRoutableIPForConnectionTimeout, newLogHandler(&defaultLogger{}, Info,
				language.English), connSettings)
			t2 := time.Since(t1)
			assert.True(t, t2.Seconds() < 2.5 && t2.Seconds() > 1.5)
			assert.NotNil(t, err)
			assert.Nil(t, connection)
		})

		t.Run("Test 3 second timeout", func(t *testing.T) {
			connSettings.connectionTimeout = 3 * time.Second
			t1 := time.Now()
			connection, err := createConnection(nonRoutableIPForConnectionTimeout, newLogHandler(&defaultLogger{}, Info,
				language.English), connSettings)
			t2 := time.Since(t1)
			assert.True(t, t2.Seconds() < 3.5 && t2.Seconds() > 2.5)
			assert.NotNil(t, err)
			assert.Nil(t, connection)
		})
	})

	t.Run("Test DriverRemoteConnection GraphTraversal", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		// Initialize graph
		g := initializeGraph(t, testNoAuthUrl, testNoAuthAuthInfo, testNoAuthTlsConfig)
		defer g.remoteConnection.Close()

		// Read test data out of the graph and check that it is correct.
		readTestDataVertexProperties(t, g)
		readTestDataValues(t, g)

		// Reset Graph
		resetGraph(t, g)
	})

	t.Run("Test createConnection", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		connection, err := createConnection(testNoAuthUrl, newLogHandler(&defaultLogger{}, Info, language.English),
			newDefaultConnectionSettings())
		assert.Nil(t, err)
		assert.NotNil(t, connection)
		assert.Equal(t, established, connection.state)
		defer deferredCleanup(t, connection)
	})

	t.Run("Test createConnection with compression", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		setting := newDefaultConnectionSettings()
		setting.enableCompression = true
		connection, err := createConnection(testNoAuthUrl, newLogHandler(&defaultLogger{}, Info, language.English),
			setting)
		assert.Nil(t, err)
		assert.NotNil(t, connection)
		assert.Equal(t, established, connection.state)
		defer deferredCleanup(t, connection)
	})

	t.Run("Test connection.write() with small buffer size", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		setting := newDefaultConnectionSettings()
		setting.readBufferSize = 100
		setting.writeBufferSize = 150
		setting.connectionTimeout = 1 * time.Second
		connection, err := createConnection(testNoAuthUrl, newLogHandler(&defaultLogger{}, Info, language.English),
			setting)
		assert.Nil(t, err)
		assert.NotNil(t, connection)
		assert.Equal(t, established, connection.state)
		defer deferredCleanup(t, connection)
		request := makeStringRequest("g.V().count()", "g", "")
		resultSet, err := connection.write(&request)
		assert.Nil(t, err)
		assert.NotNil(t, resultSet)
		result, ok, err := resultSet.One()
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.NotNil(t, result)
	})

	t.Run("Test connection.write()", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		connection, err := createConnection(testNoAuthUrl, newLogHandler(&defaultLogger{}, Info, language.English),
			newDefaultConnectionSettings())
		assert.Nil(t, err)
		assert.NotNil(t, connection)
		assert.Equal(t, established, connection.state)
		defer deferredCleanup(t, connection)
		request := makeStringRequest("g.V().count()", "g", "")
		resultSet, err := connection.write(&request)
		assert.Nil(t, err)
		assert.NotNil(t, resultSet)
		result, ok, err := resultSet.One()
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.NotNil(t, result)
	})

	t.Run("Test connection.close() failure", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		connection, err := createConnection(testNoAuthUrl, newLogHandler(&defaultLogger{}, Info, language.English),
			newDefaultConnectionSettings())
		assert.Equal(t, established, connection.state)
		assert.Nil(t, err)
		err = connection.close()
		assert.Nil(t, err)
		assert.Equal(t, closed, connection.state)
		err = connection.close()
		assert.Equal(t, newError(err0101ConnectionCloseError), err)
		assert.Equal(t, closed, connection.state)
		err = connection.close()
		assert.Equal(t, newError(err0101ConnectionCloseError), err)
		assert.Equal(t, closed, connection.state)
	})

	t.Run("Test connection.write() after close() failure", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		connection, err := createConnection(testNoAuthUrl, newLogHandler(&defaultLogger{}, Info, language.English),
			newDefaultConnectionSettings())
		assert.Equal(t, established, connection.state)
		assert.Nil(t, err)
		err = connection.close()
		assert.Nil(t, err)
		assert.Equal(t, closed, connection.state)
		request := makeStringRequest("g.V().count()", "g", "")
		resultSet, err := connection.write(&request)
		assert.Nil(t, resultSet)
		assert.Equal(t, newError(err0102WriteConnectionClosedError), err)
		assert.Equal(t, closed, connection.state)
	})

	t.Run("Test server closes websocket", func(t *testing.T) {
		skipTestsIfNotEnabled(t, manualTestSuiteName, testManual)
		connSettings := newDefaultConnectionSettings()
		connSettings.keepAliveInterval = 500 * keepAliveIntervalDefault
		connection, err := createConnection(testNoAuthUrl, newLogHandler(&defaultLogger{}, Info, language.English),
			connSettings)
		assert.Equal(t, established, connection.state)
		assert.Nil(t, err)
		time.Sleep(120 * time.Second)
		request := makeStringRequest("g.V().count()", "g", "")
		resultSet, err := connection.write(&request)
		assert.Nil(t, resultSet)
		assert.NotNil(t, err)
	})

	t.Run("Test newLoadBalancingPool", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		newPoolSize := 2
		pool, err := newLoadBalancingPool(testNoAuthUrl, newLogHandler(&defaultLogger{}, Info, language.English),
			newDefaultConnectionSettings(), 4, 4, newPoolSize)
		assert.Nil(t, err)
		defer pool.close()
		assert.Len(t, pool.(*loadBalancingPool).connections, newPoolSize)
	})

	t.Run("Test loadBalancingPool.newConnection", func(t *testing.T) {
		newPoolSize := 0
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		pool, err := newLoadBalancingPool(testNoAuthUrl, newLogHandler(&defaultLogger{}, Info, language.English),
			newDefaultConnectionSettings(), 4, 4, newPoolSize)
		assert.Nil(t, err)
		defer pool.close()
		lhp := pool.(*loadBalancingPool)
		// Pool instantiated with no connections so this will invoke newConnection
		newConn, err := lhp.getLeastUsedConnection()
		assert.Nil(t, err)
		assert.Len(t, lhp.connections, newPoolSize+1)
		// Workaround for false positive in race condition check
		found := false
		for _, conn := range lhp.connections {
			if conn == newConn {
				found = true
			}
		}
		assert.True(t, found)
	})

	t.Run("Test loadBalancingPool.getLeastUsedConnection", func(t *testing.T) {
		newConnectionThreshold := 2
		maximumConcurrentConnections := 2
		logHandler := newLogHandler(&defaultLogger{}, Info, language.English)
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		t.Run("pool is empty", func(t *testing.T) {
			pool, err := newLoadBalancingPool(testNoAuthUrl, newLogHandler(&defaultLogger{}, Info, language.English),
				newDefaultConnectionSettings(),
				newConnectionThreshold, maximumConcurrentConnections, 0)
			assert.Nil(t, err)
			lbp := pool.(*loadBalancingPool)
			defer lbp.close()
			conn, err := lbp.getLeastUsedConnection()
			assert.Nil(t, err)
			assert.NotNil(t, conn)
			assert.Len(t, lbp.connections, 1)
		})

		t.Run("newConcurrentThreshold reached with capacity remaining", func(t *testing.T) {
			pool, err := newLoadBalancingPool(testNoAuthUrl, newLogHandler(&defaultLogger{}, Info, language.English),
				newDefaultConnectionSettings(),
				newConnectionThreshold, maximumConcurrentConnections, 0)
			assert.Nil(t, err)
			lbp := pool.(*loadBalancingPool)
			defer lbp.close()
			thresholdReachedResults := &synchronizedMap{
				internalMap: map[string]ResultSet{},
				syncLock:    sync.Mutex{},
			}
			thresholdReachedResults.store("1", nil)
			thresholdReachedResults.store("2", nil)
			fullConnection := &connection{
				logHandler: logHandler,
				protocol:   nil,
				results:    thresholdReachedResults,
				state:      established,
			}
			capacityAvailablePool := make([]*connection, 0, maximumConcurrentConnections)
			capacityAvailablePool = append(capacityAvailablePool, fullConnection)
			lbp.connections = capacityAvailablePool
			conn, err := lbp.getLeastUsedConnection()
			assert.Nil(t, err)
			assert.NotNil(t, conn)
			assert.NotEqual(t, fullConnection, conn)
			assert.Len(t, lbp.connections, 2)
		})

		t.Run("newConcurrentThreshold reached with no capacity remaining", func(t *testing.T) {
			capacityFullConnectionPool, err := newLoadBalancingPool(testNoAuthUrl, newLogHandler(&defaultLogger{}, Info,
				language.English), newDefaultConnectionSettings(),
				1, 1, 1)
			assert.Nil(t, err)
			assert.NotNil(t, capacityFullConnectionPool)
			capacityFullLbp := capacityFullConnectionPool.(*loadBalancingPool)
			defer capacityFullLbp.close()
			capacityFullLbp.connections[0].results.store("mockFillCapacity",
				newChannelResultSet("mockFillCapacity", capacityFullLbp.connections[0].results))
			conn, err := capacityFullLbp.getLeastUsedConnection()
			assert.Nil(t, err)
			assert.NotNil(t, conn)
			assert.Len(t, capacityFullLbp.connections, 1)
		})

		t.Run("all connections in pool invalid", func(t *testing.T) {
			pool, err := newLoadBalancingPool(testNoAuthUrl, newLogHandler(&defaultLogger{}, Info, language.English),
				newDefaultConnectionSettings(),
				newConnectionThreshold, maximumConcurrentConnections, 0)
			assert.Nil(t, err)
			lbp := pool.(*loadBalancingPool)
			defer lbp.close()
			invalidConnection1 := &connection{
				logHandler: logHandler,
				protocol:   nil,
				results:    nil,
				state:      closed,
			}
			invalidConnection2 := &connection{
				logHandler: logHandler,
				protocol:   nil,
				results:    nil,
				state:      closedDueToError,
			}
			invalidPool := []*connection{invalidConnection1, invalidConnection2}
			lbp.connections = invalidPool
			conn, err := lbp.getLeastUsedConnection()
			assert.Nil(t, err)
			assert.NotNil(t, conn)
			assert.NotEqual(t, invalidConnection1, conn)
			assert.NotEqual(t, invalidConnection2, conn)
			assert.Len(t, lbp.connections, 1)
			assert.NotContains(t, lbp.connections, invalidConnection1)
			assert.NotContains(t, lbp.connections, invalidConnection2)
		})
	})

	t.Run("Test client.submit()", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.TlsConfig = testNoAuthTlsConfig
				settings.AuthInfo = testNoAuthAuthInfo
			})
		assert.Nil(t, err)
		assert.NotNil(t, client)
		defer client.Close()

		resultSet, err := client.Submit("g.V().count()")
		assert.Nil(t, err)
		assert.NotNil(t, resultSet)
		result, ok, err := resultSet.One()
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.NotNil(t, result)

		g := cloneGraphTraversalSource(&Graph{}, NewBytecode(nil), nil)
		b := g.V().Count().Bytecode
		resultSet, err = client.submitBytecode(b)
		assert.Nil(t, err)
		assert.NotNil(t, resultSet)
		result, ok, err = resultSet.One()
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.NotNil(t, result)
	})

	t.Run("Test client.submit() on session", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)

		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.TlsConfig = testNoAuthTlsConfig
				settings.AuthInfo = testNoAuthAuthInfo
			})
		client.session = "abc123"
		assert.Nil(t, err)
		assert.NotNil(t, client)
		defer client.Close()

		resultSet, err := client.Submit("g.V().count()")
		assert.Nil(t, err)
		assert.NotNil(t, resultSet)
		result, ok, err := resultSet.One()
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.NotNil(t, result)
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

		g := Traversal_().WithRemote(remote)

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

		g := Traversal_().WithRemote(remote)

		r, err := g.V().Count().ToList()
		assert.Nil(t, err)
		for _, res := range r {
			assert.Equal(t, int64(6), res.GetInterface())
		}
	})

	t.Run("Test Sessions", func(t *testing.T) {
		t.Run("Test CreateSessions", func(t *testing.T) {
			skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
			remote, err := NewDriverRemoteConnection(testNoAuthWithAliasUrl,
				func(settings *DriverRemoteConnectionSettings) {
					settings.TlsConfig = testNoAuthTlsConfig
					settings.AuthInfo = testNoAuthAuthInfo
				})
			assert.Nil(t, err)
			assert.NotNil(t, remote)
			defer remote.Close()

			remoteSession1, err := remote.CreateSession()
			assert.Nil(t, err)
			assert.NotNil(t, remoteSession1)
			defer remoteSession1.Close()
			assert.Equal(t, remote.client.session, "")
			assert.NotEqual(t, remoteSession1.client.session, "")
			assert.Equal(t, 1, len(remote.spawnedSessions))
			fixedUUID := uuid.New().String()

			remoteSession2, err := remote.CreateSession(fixedUUID)
			assert.Nil(t, err)
			assert.NotNil(t, remoteSession2)
			defer remoteSession2.Close()
			assert.Equal(t, remoteSession2.client.session, fixedUUID)
			assert.Equal(t, 2, len(remote.spawnedSessions))
		})

		t.Run("Test session close", func(t *testing.T) {
			skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
			remote, err := NewDriverRemoteConnection(testNoAuthWithAliasUrl,
				func(settings *DriverRemoteConnectionSettings) {
					settings.TlsConfig = testNoAuthTlsConfig
					settings.AuthInfo = testNoAuthAuthInfo
				})
			assert.Nil(t, err)
			assert.NotNil(t, remote)
			defer remote.Close()

			session1, _ := remote.CreateSession()
			assert.NotNil(t, session1.client.session)
			session1.Close()
			assert.Equal(t, 1, len(remote.spawnedSessions))
			sId := session1.GetSessionId()
			session2, _ := remote.CreateSession(sId)
			assert.NotNil(t, session2.client.session)
			session3, _ := remote.CreateSession()
			assert.NotNil(t, session3.client.session)
			assert.Equal(t, 3, len(remote.spawnedSessions))
		})

		t.Run("Test session failures", func(t *testing.T) {
			skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
			remote, err := NewDriverRemoteConnection(testNoAuthWithAliasUrl,
				func(settings *DriverRemoteConnectionSettings) {
					settings.TlsConfig = testNoAuthTlsConfig
					settings.AuthInfo = testNoAuthAuthInfo
				})
			assert.Nil(t, err)
			assert.NotNil(t, remote)
			defer remote.Close()
			s1, err := remote.CreateSession()
			assert.Nil(t, err)
			assert.NotNil(t, s1)
			defer s1.Close()
			s2, err := s1.CreateSession()
			assert.Nil(t, s2)
			assert.Equal(t, newError(err0202CreateSessionFromSessionError), err)
		})

		t.Run("Test CreateSession with multiple UUIDs failure", func(t *testing.T) {
			skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
			remote, err := NewDriverRemoteConnection(testNoAuthWithAliasUrl,
				func(settings *DriverRemoteConnectionSettings) {
					settings.TlsConfig = testNoAuthTlsConfig
					settings.AuthInfo = testNoAuthAuthInfo
				})
			assert.Nil(t, err)
			assert.NotNil(t, remote)
			defer remote.Close()
			s1, err := remote.CreateSession(uuid.New().String(), uuid.New().String())
			assert.Nil(t, s1)
			assert.Equal(t, newError(err0201CreateSessionMultipleIdsError), err)
		})
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

		resultSet, err := client.Submit("x + x", map[string]interface{}{"x": 2})
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
		g := Traversal_().WithRemote(remote)

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
		assert.True(t, isSameErrorCode(newError(err0502ResponseHandlerReadLoopError), err))

		rs, err = g.V().Count().ToList()
		assert.NotNil(t, rs)
		assert.Nil(t, err)

		// Drop the graph.
		dropGraph(t, g)
	})

	// This test needs to be run as a standalone since other tests running can cause goroutine count to fluctuate.
	// If this test is not run manually and isolated it will have floating failures.
	t.Run("Test connection goroutine cleanup", func(t *testing.T) {
		skipTestsIfNotEnabled(t, manualTestSuiteName, testManual)

		startCount := runtime.NumGoroutine()

		connection, err := createConnection(testNoAuthUrl, newLogHandler(&defaultLogger{}, Info, language.English),
			newDefaultConnectionSettings())
		assert.Nil(t, err)
		assert.NotNil(t, connection)
		assert.Equal(t, established, connection.state)

		// Read loop, write loop, this routine.
		assert.Equal(t, startCount+2, runtime.NumGoroutine())

		assert.Nil(t, connection.close())

		// This routine.
		assert.Equal(t, startCount, runtime.NumGoroutine())
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
}
