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
	"testing"

	"github.com/stretchr/testify/assert"
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
		resultSet, err := client.Submit("g.inject(2)")
		assert.NoError(t, err)
		assert.NotNil(t, resultSet)

		client.Close()
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

	t.Run("Test client.Submit() with bindings", func(t *testing.T) {
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

		bindings := map[string]interface{}{"x": 1}

		resultSet, err := client.Submit("g.V(x).values(\"name\")", bindings)
		assert.NoError(t, err)
		assert.NotNil(t, resultSet)

		result, ok, err := resultSet.One()
		assert.NoError(t, err)
		assert.True(t, ok)

		assert.Equal(t, "marko", result.Data)
	})

	t.Run("Test client.SubmitWithOptions() with bindings", func(t *testing.T) {
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

		bindings := map[string]interface{}{"x": 1}

		resultSet, err := client.SubmitWithOptions("g.V(x).values(\"name\")", new(RequestOptionsBuilder).SetBindings(bindings).Create())
		assert.NoError(t, err)
		assert.NotNil(t, resultSet)

		result, ok, err := resultSet.One()
		assert.NoError(t, err)
		assert.True(t, ok)

		assert.Equal(t, "marko", result.Data)
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
