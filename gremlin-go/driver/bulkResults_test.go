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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newBulkResultsCaptureServer returns an httptest server that captures the JSON
// request body of the most recent submission, plus a pointer to the captured map.
func newBulkResultsCaptureServer(t *testing.T) (*httptest.Server, *map[string]interface{}) {
	t.Helper()
	captured := new(map[string]interface{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err == nil && len(body) > 0 {
			var parsed map[string]interface{}
			if json.Unmarshal(body, &parsed) == nil {
				*captured = parsed
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	return server, captured
}

// TestConnectionLevelBulkResults verifies the connection-level BulkResults setting,
// which matches the other GLVs: a plain bool defaulting to false. When true it is
// applied to every request unless overridden per-request; the DriverRemoteConnection
// traversal path independently defaults to true regardless of the setting.
func TestConnectionLevelBulkResults(t *testing.T) {
	t.Run("connection-level BulkResults=true sends bulkResults=true on script path", func(t *testing.T) {
		server, captured := newBulkResultsCaptureServer(t)
		defer server.Close()

		client, err := NewClient(server.URL, func(settings *ClientSettings) {
			settings.BulkResults = true
		})
		require.NoError(t, err)
		defer client.Close()

		rs, err := client.SubmitWithOptions("g.V()", *new(RequestOptions))
		require.NoError(t, err)
		_, _ = rs.All()

		assert.Equal(t, true, (*captured)["bulkResults"],
			"connection-level BulkResults=true should be sent on the script path")
	})

	t.Run("per-request SetBulkResults(false) overrides connection-level true", func(t *testing.T) {
		server, captured := newBulkResultsCaptureServer(t)
		defer server.Close()

		client, err := NewClient(server.URL, func(settings *ClientSettings) {
			settings.BulkResults = true
		})
		require.NoError(t, err)
		defer client.Close()

		opts := new(RequestOptionsBuilder).SetBulkResults(false).Create()
		rs, err := client.SubmitWithOptions("g.V()", opts)
		require.NoError(t, err)
		_, _ = rs.All()

		assert.Equal(t, false, (*captured)["bulkResults"],
			"per-request SetBulkResults(false) should override a connection-level true")
	})

	t.Run("default (false) connection-level leaves script-path bulkResults unset", func(t *testing.T) {
		server, captured := newBulkResultsCaptureServer(t)
		defer server.Close()

		client, err := NewClient(server.URL)
		require.NoError(t, err)
		defer client.Close()

		rs, err := client.SubmitWithOptions("g.V()", *new(RequestOptions))
		require.NoError(t, err)
		_, _ = rs.All()

		_, present := (*captured)["bulkResults"]
		assert.False(t, present,
			"with the default (false) connection-level setting and no per-request value, bulkResults should not be sent on the script path")
	})

	t.Run("DRC traversal path defaults bulkResults to true regardless of connection-level false", func(t *testing.T) {
		server, captured := newBulkResultsCaptureServer(t)
		defer server.Close()

		// Connection-level BulkResults defaults to false, but the DRC traversal
		// path still defaults to true, matching the other GLVs.
		client, err := NewClient(server.URL)
		require.NoError(t, err)
		defer client.Close()

		rs, err := client.submitGremlinLang(NewGremlinLang(nil))
		require.NoError(t, err)
		_, _ = rs.All()

		assert.Equal(t, true, (*captured)["bulkResults"],
			"the DRC traversal path should default bulkResults to true")
	})
}
