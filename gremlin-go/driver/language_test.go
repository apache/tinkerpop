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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// submitAndCapture runs the traversal built by build against a capture server and returns the request body.
func submitAndCapture(t *testing.T, configure func(*DriverRemoteConnectionSettings),
	build func(g *GraphTraversalSource) *GraphTraversal) map[string]interface{} {
	t.Helper()
	server, captured := newBulkResultsCaptureServer(t)
	defer server.Close()

	drc, err := NewDriverRemoteConnection(server.URL, configure)
	require.NoError(t, err)
	defer drc.Close()

	g := Traversal_().With(drc)
	// the capture server returns an empty response so the result is not interesting
	_, _ = build(g).ToList()
	return *captured
}

func TestRequestLanguage(t *testing.T) {
	v := func(g *GraphTraversalSource) *GraphTraversal { return g.V() }

	t.Run("sends gremlin-lang when no language is configured", func(t *testing.T) {
		body := submitAndCapture(t, func(s *DriverRemoteConnectionSettings) {}, v)
		assert.Equal(t, "gremlin-lang", body["language"])
	})

	t.Run("sends the connection-level language", func(t *testing.T) {
		body := submitAndCapture(t, func(s *DriverRemoteConnectionSettings) {
			s.RequestLanguage = "my-engine"
		}, v)
		assert.Equal(t, "my-engine", body["language"])
	})

	t.Run("With takes precedence over the connection-level language", func(t *testing.T) {
		body := submitAndCapture(t, func(s *DriverRemoteConnectionSettings) {
			s.RequestLanguage = "connection-engine"
		}, func(g *GraphTraversalSource) *GraphTraversal { return g.With("language", "source-engine").V() })
		assert.Equal(t, "source-engine", body["language"])
	})

	t.Run("does not render language into the gremlin text", func(t *testing.T) {
		body := submitAndCapture(t, func(s *DriverRemoteConnectionSettings) {},
			func(g *GraphTraversalSource) *GraphTraversal { return g.With("language", "my-engine").V() })
		assert.NotContains(t, body["gremlin"], "language")
	})
}

func TestRequestLanguageIntegration(t *testing.T) {
	testNoAuthUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	testNoAuthEnable := getEnvOrDefaultBool("RUN_INTEGRATION_TESTS", true)

	newConnection := func(t *testing.T, language string) *DriverRemoteConnection {
		t.Helper()
		drc, err := NewDriverRemoteConnection(testNoAuthUrl, func(settings *DriverRemoteConnectionSettings) {
			settings.TraversalSource = testServerModernGraphAlias
			settings.RequestLanguage = language
		})
		require.NoError(t, err)
		return drc
	}

	t.Run("connection-level language is used by the server", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		drc := newConnection(t, "gremlin-lang")
		defer drc.Close()

		results, err := Traversal_().With(drc).Inject(1).ToList()
		require.NoError(t, err)
		require.Len(t, results, 1)
	})

	t.Run("connection-level language is sent to the server", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		drc := newConnection(t, "no-such-engine")
		defer drc.Close()

		_, err := Traversal_().With(drc).Inject(1).ToList()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Script engine [no-such-engine] is not available")
	})

	t.Run("With language is sent to the server", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		drc := newConnection(t, "")
		defer drc.Close()

		_, err := Traversal_().With(drc).With("language", "no-such-engine").Inject(1).ToList()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Script engine [no-such-engine] is not available")
	})

	t.Run("With language overrides the connection-level language", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, testNoAuthEnable)
		drc := newConnection(t, "no-such-engine")
		defer drc.Close()

		results, err := Traversal_().With(drc).With("language", "gremlin-lang").Inject(1).ToList()
		require.NoError(t, err)
		require.Len(t, results, 1)
	})
}
