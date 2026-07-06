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
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	// Integration test variables.
	testNoAuthUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	testNoAuthEnable := getEnvOrDefaultBool("RUN_INTEGRATION_TESTS", true)
	testNoAuthTlsConfig := &tls.Config{}

	t.Run("Test client.SubmitWithOptions()", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.Ssl = testNoAuthTlsConfig
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
				settings.Ssl = testNoAuthTlsConfig
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
				settings.Ssl = testNoAuthTlsConfig
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
				settings.Ssl = testNoAuthTlsConfig
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
				settings.Ssl = testNoAuthTlsConfig
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
				settings.Ssl = testNoAuthTlsConfig
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

	t.Run("Test client.SubmitWithOptions() with bulkResults true", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.Ssl = testNoAuthTlsConfig
			})
		assert.NoError(t, err)
		assert.NotNil(t, client)
		defer client.Close()

		resultSet, err := client.SubmitWithOptions("g.inject(1,2,3,2,1)",
			new(RequestOptionsBuilder).SetBulkResults(true).Create())
		assert.NoError(t, err)
		assert.NotNil(t, resultSet)
		results, err := resultSet.All()
		assert.NoError(t, err)
		// With bulkResults=true, the ResultSet contains Traverser objects (one per unique value).
		// This is consistent with Java, Python, .NET, and JS which all expose Traverser objects
		// at the ResultSet level when bulking is enabled.
		// g.inject(1,2,3,2,1) has 3 unique values: 1 (bulk=2), 2 (bulk=2), 3 (bulk=1)
		assert.Equal(t, 3, len(results))
		totalBulk := int64(0)
		for _, r := range results {
			tr, err := r.GetTraverser()
			assert.NoError(t, err)
			totalBulk += tr.Bulk
		}
		assert.Equal(t, int64(5), totalBulk)
	})

	t.Run("Test client.SubmitWithOptions() with bulkResults false", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.Ssl = testNoAuthTlsConfig
			})
		assert.NoError(t, err)
		assert.NotNil(t, client)
		defer client.Close()

		resultSet, err := client.SubmitWithOptions("g.inject(1,2,3,2,1)",
			new(RequestOptionsBuilder).SetBulkResults(false).Create())
		assert.NoError(t, err)
		assert.NotNil(t, resultSet)
		results, err := resultSet.All()
		assert.NoError(t, err)
		// With bulkResults=false, the ResultSet contains raw values (no Traverser wrapping).
		assert.Equal(t, 5, len(results))
		for _, r := range results {
			_, err := r.GetTraverser()
			assert.Error(t, err, "expected raw value, not Traverser")
		}
	})

	t.Run("Test deserialization of VertexProperty with properties", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testNoAuthUrl,
			func(settings *ClientSettings) {
				settings.Ssl = testNoAuthTlsConfig
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

func TestCompositePDTIntegration(t *testing.T) {
	testNoAuthUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	testNoAuthEnable := getEnvOrDefaultBool("RUN_INTEGRATION_TESTS", true)

	t.Run("simple Point PDT round-trip", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testNoAuthUrl, func(settings *ClientSettings) {
			settings.Ssl = &tls.Config{}
			settings.TraversalSource = testServerModernGraphAlias
		})
		require.NoError(t, err)
		defer client.Close()

		rs, err := client.Submit("g.inject(PDT(\"Point\", [\"x\":1, \"y\":2]))")
		require.NoError(t, err)

		result, ok, err := rs.One()
		require.NoError(t, err)
		require.True(t, ok)

		pdt, ok := result.Data.(*CompositePDT)
		require.True(t, ok, "expected *CompositePDT, got %T", result.Data)
		assert.Equal(t, "Point", pdt.Name)
		assert.Equal(t, int32(1), pdt.Fields["x"])
		assert.Equal(t, int32(2), pdt.Fields["y"])
	})

	t.Run("nested PDT (Person with Address)", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testNoAuthUrl, func(settings *ClientSettings) {
			settings.Ssl = &tls.Config{}
			settings.TraversalSource = testServerModernGraphAlias
		})
		require.NoError(t, err)
		defer client.Close()

		rs, err := client.Submit(
			"g.inject(PDT(\"Person\", [\"name\":\"Alice\", \"age\":30, " +
				"\"address\":PDT(\"Address\", [\"street\":\"123 Main St\", \"city\":\"Springfield\", \"zip\":\"12345\"])]))")
		require.NoError(t, err)

		result, ok, err := rs.One()
		require.NoError(t, err)
		require.True(t, ok)

		pdt, ok := result.Data.(*CompositePDT)
		require.True(t, ok, "expected *CompositePDT, got %T", result.Data)
		assert.Equal(t, "Person", pdt.Name)
		assert.Equal(t, "Alice", pdt.Fields["name"])
		assert.Equal(t, int32(30), pdt.Fields["age"])

		address, ok := pdt.Fields["address"].(*CompositePDT)
		require.True(t, ok, "expected nested *CompositePDT for address")
		assert.Equal(t, "Address", address.Name)
		assert.Equal(t, "123 Main St", address.Fields["street"])
		assert.Equal(t, "Springfield", address.Fields["city"])
		assert.Equal(t, "12345", address.Fields["zip"])
	})

	t.Run("PDT in collection", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		client, err := NewClient(testNoAuthUrl, func(settings *ClientSettings) {
			settings.Ssl = &tls.Config{}
			settings.TraversalSource = testServerModernGraphAlias
		})
		require.NoError(t, err)
		defer client.Close()

		rs, err := client.Submit(
			"g.inject([PDT(\"Point\", [\"x\":1, \"y\":2]), PDT(\"Point\", [\"x\":3, \"y\":4])])")
		require.NoError(t, err)

		result, ok, err := rs.One()
		require.NoError(t, err)
		require.True(t, ok)

		list, ok := result.Data.([]interface{})
		require.True(t, ok, "expected []interface{}, got %T", result.Data)
		require.Len(t, list, 2)

		p1, ok := list[0].(*CompositePDT)
		require.True(t, ok)
		assert.Equal(t, "Point", p1.Name)
		assert.Equal(t, int32(1), p1.Fields["x"])
		assert.Equal(t, int32(2), p1.Fields["y"])

		p2, ok := list[1].(*CompositePDT)
		require.True(t, ok)
		assert.Equal(t, "Point", p2.Name)
		assert.Equal(t, int32(3), p2.Fields["x"])
		assert.Equal(t, int32(4), p2.Fields["y"])
	})
}
