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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMatchGqlIntegration verifies that the GraphTraversalSource.Match(String) spawn step
// correctly issues a GQL pattern-match query against a TinkerGraph modern graph and returns
// the expected results. This is a one-off integration test; it requires a running Gremlin
// Server with the modern graph loaded (alias "gmodern") and
// TinkerGraphDeclarativeMatchStrategy registered (automatic for TinkerGraph).
func TestMatchGqlIntegration(t *testing.T) {
	testNoAuthUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)

	t.Run("Match GQL person-knows-person pattern on modern graph", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true))

		g := getModernGraph(t, testNoAuthUrl, &tls.Config{})
		defer g.remoteConnection.Close()

		// The modern graph contains exactly two "knows" edges out of marko:
		//   marko -[knows]-> vadas
		//   marko -[knows]-> josh
		// g.Match("MATCH (p:person)-[e:knows]->(friend:person)").Select("p","friend")
		// should therefore return two rows.
		results, err := g.Match("MATCH (p:person)-[e:knows]->(friend:person)").Select("p", "friend").ToList()
		require.Nil(t, err)
		require.Equal(t, 2, len(results), "expected exactly 2 person-knows-person pairs in the modern graph")

		var friendNames []string
		for _, result := range results {
			row, ok := result.GetInterface().(map[interface{}]interface{})
			require.True(t, ok, "expected each select result to be a map")

			pVal, pExists := row["p"]
			require.True(t, pExists, "expected 'p' key in result map")
			pVertex, ok := pVal.(*Vertex)
			require.True(t, ok, "expected 'p' value to be a *Vertex")
			assert.Equal(t, "person", pVertex.Label)

			friendVal, friendExists := row["friend"]
			require.True(t, friendExists, "expected 'friend' key in result map")
			friendVertex, ok := friendVal.(*Vertex)
			require.True(t, ok, "expected 'friend' value to be a *Vertex")
			assert.Equal(t, "person", friendVertex.Label)

			// Extract the friend's name from its vertex properties.
			props, ok := friendVertex.Properties.([]interface{})
			if ok {
				for _, p := range props {
					if vp, ok := p.(*VertexProperty); ok && vp.Label == "name" {
						friendNames = append(friendNames, vp.Value.(string))
					}
				}
			}
		}

		// Both known friends of marko must appear in the results.
		sort.Strings(friendNames)
		assert.Equal(t, []string{"josh", "vadas"}, friendNames)
	})
}
