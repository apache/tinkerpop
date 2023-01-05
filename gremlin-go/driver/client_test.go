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
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"testing"
)

func TestClient(t *testing.T) {
	// Integration test variables.
	testNoAuthUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	testNoAuthEnable := getEnvOrDefaultBool("RUN_INTEGRATION_TESTS", true)
	testNoAuthAuthInfo := &AuthInfo{}
	testNoAuthTlsConfig := &tls.Config{}

	t.Run("Test client.Submit()", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.TlsConfig = testNoAuthTlsConfig
				settings.AuthInfo = testNoAuthAuthInfo
			})
		defer client.Close()
		assert.Nil(t, err)
		assert.NotNil(t, client)
		resultSet, err := client.Submit("g.V().count()")
		assert.Nil(t, err)
		assert.NotNil(t, resultSet)
		result, ok, err := resultSet.One()
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.NotNil(t, result)
	})

	t.Run("Test client.SubmitWithOptions()", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.TlsConfig = testNoAuthTlsConfig
				settings.AuthInfo = testNoAuthAuthInfo
			})
		defer client.Close()
		assert.Nil(t, err)
		assert.NotNil(t, client)
		resultSet, err := client.SubmitWithOptions("g.V().count()", *new(RequestOptions))
		assert.Nil(t, err)
		assert.NotNil(t, resultSet)
		result, ok, err := resultSet.One()
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.NotNil(t, result)
	})
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
		defer client.Close()
		assert.Nil(t, err)
		assert.NotNil(t, client)
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
		defer client.Close()
		assert.Nil(t, err)
		assert.NotNil(t, client)

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
		defer client.Close()
		assert.Nil(t, err)
		assert.NotNil(t, client)

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
		defer client.Close()
		assert.Nil(t, err)
		assert.NotNil(t, client)

		resultSet, err := client.SubmitWithOptions("1", new(RequestOptionsBuilder).
			SetRequestId(settings.PER_REQUEST_SETTINGS_REQUEST_ID).
			SetEvaluationTimeout(1234).
			SetBatchSize(12).
			SetUserAgent("helloWorld").
			Create())
		assert.Nil(t, err)
		assert.NotNil(t, resultSet)
		result, ok, err := resultSet.One()
		assert.Nil(t, err)
		assert.True(t, ok)
		expectedResult := fmt.Sprintf("requestId=%v evaluationTimeout=%v, batchSize=%v, userAgent=%v",
			settings.PER_REQUEST_SETTINGS_REQUEST_ID, 1234, 12, "helloWorld")
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
