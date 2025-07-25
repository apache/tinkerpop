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
	"log"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestClient(t *testing.T) {
	// Integration test variables.
	testNoAuthUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	testNoAuthEnable := getEnvOrDefaultBool("RUN_INTEGRATION_TESTS", true)
	testNoAuthAuthInfo := &AuthInfo{}
	testNoAuthTlsConfig := &tls.Config{}

	t.Run("Test client.SubmitWithOptions()", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.TlsConfig = testNoAuthTlsConfig
				settings.AuthInfo = testNoAuthAuthInfo
			})
		assert.NoError(t, err)
		assert.NotNil(t, client)
		defer client.Close()

		resultSet, err := client.SubmitWithOptions("g.V().count()", *new(RequestOptions))
		assert.NoError(t, err)
		assert.NotNil(t, resultSet)
		result, ok, err := resultSet.One()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.NotNil(t, result)
	})

	t.Run("Test client.Close()", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.TlsConfig = testNoAuthTlsConfig
				settings.AuthInfo = testNoAuthAuthInfo
			})
		assert.NoError(t, err)
		assert.NotNil(t, client)
		resultSet, err := client.Submit("2+2")
		assert.NoError(t, err)
		assert.NotNil(t, resultSet)

		client.Close()
		pool := client.connections.(*loadBalancingPool)
		assert.Equal(t, 1, len(pool.connections))
		assert.True(t, pool.isClosed)
		assert.Equal(t, closed, pool.connections[0].state)
	})

	t.Run("Test client.Submit()", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.TlsConfig = testNoAuthTlsConfig
				settings.AuthInfo = testNoAuthAuthInfo
				settings.TraversalSource = testServerModernGraphAlias
			})
		assert.NoError(t, err)
		assert.NotNil(t, client)
		defer client.Close()

		resultSet, err := client.Submit("g.V(1)")
		assert.NoError(t, err)
		assert.NotNil(t, resultSet)

		result, ok, err := resultSet.One()
		assert.NoError(t, err)
		assert.True(t, ok)

		AssertMarkoVertexWithProperties(t, result)
	})

	t.Run("Test client.submit() with materializeProperties", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.TlsConfig = testNoAuthTlsConfig
				settings.AuthInfo = testNoAuthAuthInfo
				settings.TraversalSource = testServerModernGraphAlias
			})

		assert.NoError(t, err)
		assert.NotNil(t, client)
		defer client.Close()

		resultSet, err := client.Submit("g.with('materializeProperties', 'tokens').V(1)")
		assert.NoError(t, err)
		assert.NotNil(t, resultSet)
		result, ok, err := resultSet.One()
		assert.NoError(t, err)
		assert.True(t, ok)

		AssertMarkoVertexWithoutProperties(t, result)
	})

	t.Run("Test deserialization of VertexProperty with properties", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.TlsConfig = testNoAuthTlsConfig
				settings.AuthInfo = testNoAuthAuthInfo
				settings.TraversalSource = testServerCrewGraphAlias
			})

		assert.NoError(t, err)
		assert.NotNil(t, client)
		defer client.Close()

		resultSet, err := client.Submit("g.V(7)")
		assert.NoError(t, err)
		assert.NotNil(t, resultSet)
		result, ok, err := resultSet.One()
		assert.NoError(t, err)
		assert.True(t, ok)

		AssertVertexPropertiesWithProperties(t, result)
	})

	t.Run("Test sessioned client", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.TlsConfig = testNoAuthTlsConfig
				settings.AuthInfo = testNoAuthAuthInfo
				settings.TraversalSource = testServerModernGraphAlias
				settings.Session = "sessionID"
			})
		assert.NoError(t, err)
		assert.NotNil(t, client)
		defer client.Close()

		resultSet, err := client.Submit("x = 1+1")
		assert.NoError(t, err)
		assert.NotNil(t, resultSet)

		result, ok, err := resultSet.One()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.EqualValues(t, 2, result.Data)

		resultSet, err = client.Submit("x+1")
		assert.NoError(t, err)
		assert.NotNil(t, resultSet)

		result, ok, err = resultSet.One()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.EqualValues(t, 3, result.Data)
	})
}

func AssertVertexPropertiesWithProperties(t *testing.T, result *Result) {
	assert.NotNil(t, result)

	vertex, err := result.GetVertex()
	assert.NoError(t, err)
	assert.NotNil(t, vertex)

	properties, ok := vertex.Properties.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 4, len(properties))

	property, ok := properties[1].(*VertexProperty)
	assert.True(t, ok)
	assert.NotNil(t, property)
	assert.Equal(t, "centreville", property.Value)
	vertexPropertyProperties := property.Properties.([]interface{})
	assert.Equal(t, 2, len(vertexPropertyProperties))
	assert.Equal(t, "startTime", (vertexPropertyProperties[0].(*Property)).Key)
	assert.Equal(t, int32(1990), (vertexPropertyProperties[0].(*Property)).Value)
	assert.Equal(t, "endTime", (vertexPropertyProperties[1].(*Property)).Key)
	assert.Equal(t, int32(2000), (vertexPropertyProperties[1].(*Property)).Value)
}

func AssertMarkoVertexWithProperties(t *testing.T, result *Result) {
	assert.NotNil(t, result)

	vertex, err := result.GetVertex()
	assert.NoError(t, err)
	assert.NotNil(t, vertex)

	properties, ok := vertex.Properties.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 2, len(properties))

	property, ok := properties[0].(*VertexProperty)
	assert.True(t, ok)
	assert.NotNil(t, property)
	assert.Equal(t, "name", property.Label)
	assert.Equal(t, "marko", property.Value)

	property, ok = properties[1].(*VertexProperty)
	assert.True(t, ok)
	assert.NotNil(t, property)
	assert.Equal(t, "age", property.Label)
	assert.Equal(t, int32(29), property.Value)
}

func AssertMarkoVertexWithoutProperties(t *testing.T, result *Result) {
	assert.NotNil(t, result)

	vertex, err := result.GetVertex()
	assert.NoError(t, err)
	assert.NotNil(t, vertex)

	properties, ok := vertex.Properties.([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 0, len(properties))
}

// Client is used to connect and interact with a Gremlin-supported server.
type SocketServerSettings struct {
	PORT int `yaml:"PORT"`
	/**
	 * Configures which serializer will be used. Ex: "GraphBinaryV1" or "GraphSONV2"
	 */
	SERIALIZER string `yaml:"SERIALIZER"`
	/**
	 * If a request with this ID comes to the server, the server responds back with a single vertex picked from Modern
	 * graph.
	 */
	SINGLE_VERTEX_REQUEST_ID uuid.UUID `yaml:"SINGLE_VERTEX_REQUEST_ID"`
	/**
	 * If a request with this ID comes to the server, the server responds back with a single vertex picked from Modern
	 * graph. After a 2 second delay, server sends a Close WebSocket frame on the same connection.
	 */
	SINGLE_VERTEX_DELAYED_CLOSE_CONNECTION_REQUEST_ID uuid.UUID `yaml:"SINGLE_VERTEX_DELAYED_CLOSE_CONNECTION_REQUEST_ID"`
	/**
	 * Server waits for 1 second, then responds with a 500 error status code
	 */
	FAILED_AFTER_DELAY_REQUEST_ID uuid.UUID `yaml:"FAILED_AFTER_DELAY_REQUEST_ID"`
	/**
	 * Server waits for 1 second then responds with a close web socket frame
	 */
	CLOSE_CONNECTION_REQUEST_ID uuid.UUID `yaml:"CLOSE_CONNECTION_REQUEST_ID"`
	/**
	 * Same as CLOSE_CONNECTION_REQUEST_ID
	 */
	CLOSE_CONNECTION_REQUEST_ID_2 uuid.UUID `yaml:"CLOSE_CONNECTION_REQUEST_ID_2"`
	/**
	 * If a request with this ID comes to the server, the server responds with the user agent (if any) that was captured
	 * during the web socket handshake.
	 */
	USER_AGENT_REQUEST_ID uuid.UUID `yaml:"USER_AGENT_REQUEST_ID"`
	/**
	 * If a request with this ID comes to the server, the server responds with a string containing all overridden
	 * per request settings from the request message. String will be of the form
	 * "requestId=19436d9e-f8fc-4b67-8a76-deec60918424 evaluationTimeout=1234, batchSize=12, userAgent=testUserAgent"
	 */
	PER_REQUEST_SETTINGS_REQUEST_ID uuid.UUID `yaml:"PER_REQUEST_SETTINGS_REQUEST_ID"`
}

func FromYaml(path string) *SocketServerSettings {
	socketServerSettings := new(SocketServerSettings)
	f, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	if err := yaml.Unmarshal(f, socketServerSettings); err != nil {
		log.Fatal(err)
	}
	return socketServerSettings
}

func TestClientAgainstSocketServer(t *testing.T) {
	// Integration test variables.
	testNoAuthEnable := getEnvOrDefaultBool("RUN_INTEGRATION_TESTS", true)
	settings := FromYaml(getEnvOrDefaultString("GREMLIN_SOCKET_SERVER_CONFIG_PATH", "../../gremlin-tools/gremlin-socket-server/conf/test-ws-gremlin.yaml"))
	testSocketServerUrl := getEnvOrDefaultString("GREMLIN_SOCKET_SERVER_URL", "ws://localhost")
	testSocketServerUrl = fmt.Sprintf("%s:%v/gremlin", testSocketServerUrl, settings.PORT)

	/**
	 * Note: This test does not demonstrate anything useful other than the ability to connect to and
	 * use gremlin-socket-server. Currently, implementing more useful tests are blocked by TINKERPOP-2845.
	 * This test can be safely removed once more interesting tests have been added which utilize
	 * gremlin-socket-server.
	 */
	t.Run("Should get single vertex response from gremlin socket server", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testSocketServerUrl)
		assert.Nil(t, err)
		assert.NotNil(t, client)
		defer client.Close()
		resultSet, err := client.SubmitWithOptions("1", new(RequestOptionsBuilder).
			SetRequestId(settings.SINGLE_VERTEX_REQUEST_ID).Create())
		assert.Nil(t, err)
		assert.NotNil(t, resultSet)
		result, ok, err := resultSet.One()
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.NotNil(t, result)
	})

	/**
	 * Tests that client is correctly sending user agent during web socket handshake by having the server return
	 * the captured user agent.
	 */
	t.Run("Should include user agent in handshake request", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testSocketServerUrl)
		assert.Nil(t, err)
		assert.NotNil(t, client)
		defer client.Close()

		resultSet, err := client.SubmitWithOptions("1", new(RequestOptionsBuilder).
			SetRequestId(settings.USER_AGENT_REQUEST_ID).Create())
		assert.Nil(t, err)
		assert.NotNil(t, resultSet)

		result, ok, err := resultSet.One()
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.NotNil(t, result)

		userAgentResponse := result.GetString()
		assert.Equal(t, userAgent, userAgentResponse)
	})

	/**
	 * Tests that no user agent (other than the default one provided by gorilla) is sent to server when
	 * that behaviour is disabled.
	 */
	t.Run("Should not include user agent in handshake request if disabled", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testSocketServerUrl,
			func(settings *ClientSettings) {
				settings.EnableUserAgentOnConnect = false
			})
		assert.Nil(t, err)
		assert.NotNil(t, client)
		defer client.Close()

		resultSet, err := client.SubmitWithOptions("1", new(RequestOptionsBuilder).
			SetRequestId(settings.USER_AGENT_REQUEST_ID).Create())
		assert.Nil(t, err)
		assert.NotNil(t, resultSet)

		result, ok, err := resultSet.One()
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.NotNil(t, result)

		userAgentResponse := result.GetString()
		//If the gremlin user agent is disabled, the underlying web socket library reverts to sending its default user agent
		//during connection requests.
		assert.Contains(t, userAgentResponse, "Go-http-client/")
	})

	/**
	 * Tests that client is correctly sending all overridable per request settings (requestId, batchSize,
	 * evaluationTimeout, and userAgent) to the server.
	 */
	t.Run("Should Send Per Request Settings To Server", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testSocketServerUrl)
		assert.Nil(t, err)
		assert.NotNil(t, client)
		defer client.Close()

		resultSet, err := client.SubmitWithOptions("1", new(RequestOptionsBuilder).
			SetRequestId(settings.PER_REQUEST_SETTINGS_REQUEST_ID).
			SetEvaluationTimeout(1234).
			SetBatchSize(12).
			SetUserAgent("helloWorld").
			SetMaterializeProperties("tokens").
			Create())
		assert.Nil(t, err)
		assert.NotNil(t, resultSet)
		result, ok, err := resultSet.One()
		assert.Nil(t, err)
		assert.True(t, ok)
		expectedResult := fmt.Sprintf("requestId=%v evaluationTimeout=%v, batchSize=%v, userAgent=%v, materializeProperties=%v",
			settings.PER_REQUEST_SETTINGS_REQUEST_ID, 1234, 12, "helloWorld", "tokens")
		assert.Equal(t, expectedResult, result.Data)
	})

	/**
	 * Note: This test currently fails due to race condition check in go test and is only included for demonstration
	 * purposes. See https://issues.apache.org/jira/browse/TINKERPOP-2845.
	 * This test should be uncommented with the resolution of TINKERPOP-2845
	 */
	/*
		t.Run("Should try create new connection if closed by server", func(t *testing.T) {
			skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
			client, err := NewClient(testSocketServerUrl)
			defer client.Close()
			assert.Nil(t, err)
			assert.NotNil(t, client)
			resultSet, err := client.SubmitWithOptions("1", new(RequestOptionsBuilder).
				SetRequestId(settings.CLOSE_CONNECTION_REQUEST_ID).Create())
			assert.Nil(t, err)
			assert.NotNil(t, resultSet)

			result, ok, err := resultSet.One()

			assert.EqualError(t, err, "websocket: close 1005 (no status)")

			resultSet, err = client.SubmitWithOptions("1", new(RequestOptionsBuilder).
				SetRequestId(settings.SINGLE_VERTEX_REQUEST_ID).Create())
			assert.Nil(t, err)
			assert.NotNil(t, resultSet)
			result, ok, err = resultSet.One()
			assert.Nil(t, err)
			assert.True(t, ok)
			assert.NotNil(t, result)
		})
	*/
}
