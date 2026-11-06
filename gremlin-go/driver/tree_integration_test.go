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

// TestTreeIntegration runs tree() against the modern graph on a live server and
// then navigates the returned native *Tree with its read API. This exercises
// the user-facing tree-shaped API that the shared gherkin feature scenarios
// cannot express.
//
// g.V(1).Out().Out().Tree().By("name") over the modern graph yields the paths
// marko -> josh -> lop and marko -> josh -> ripple, so the resulting tree is:
//
//	marko
//	   josh
//	      lop
//	      ripple
func TestTreeIntegration(t *testing.T) {
	testNoAuthUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	skipTestsIfNotEnabled(t, integrationTestSuiteName, getEnvOrDefaultBool("RUN_INTEGRATION_TESTS", true))

	g := getModernGraph(t, testNoAuthUrl, &tls.Config{})
	defer g.remoteConnection.Close()

	results, err := g.V(1).Out().Out().Tree().By("name").ToList()
	assert.NoError(t, err)
	assert.Len(t, results, 1)

	// ToList unrolls Traverser bulk wrapping via nextValue, so the result item
	// is the bare *Tree value (no manual Traverser unwrap required).
	tree, ok := results[0].GetInterface().(*Tree)
	assert.True(t, ok, "expected *Tree, got %T", results[0].GetInterface())
	assert.NotNil(t, tree)

	// Single root: marko.
	roots := tree.RootNodes()
	assert.Len(t, roots, 1)
	assert.Equal(t, "marko", roots[0])

	// The marko subtree has a single child: josh.
	markoSubtree, err := tree.ChildAt("marko")
	assert.NoError(t, err)
	assert.NotNil(t, markoSubtree)
	markoChildren := markoSubtree.RootNodes()
	assert.Len(t, markoChildren, 1)
	assert.Equal(t, "josh", markoChildren[0])

	// ChildAt for a missing key returns an error.
	_, err = tree.ChildAt("nonexistent")
	assert.Error(t, err)

	// Total node count: marko + josh + lop + ripple.
	assert.Equal(t, 4, tree.NodeCount())

	// Depth 0 is the root (marko); depth 2 holds josh's children (lop, ripple).
	depth0 := tree.GetNodesAtDepth(0)
	assert.Len(t, depth0, 1)
	assert.Contains(t, depth0, "marko")

	depth2 := tree.GetNodesAtDepth(2)
	assert.Len(t, depth2, 2)
	assert.Contains(t, depth2, "lop")
	assert.Contains(t, depth2, "ripple")

	// Leaves: lop and ripple (order-insensitive for siblings).
	leaves := tree.GetLeafNodes()
	assert.Len(t, leaves, 2)
	assert.Contains(t, leaves, "lop")
	assert.Contains(t, leaves, "ripple")

	// A non-empty tree is not a leaf.
	assert.False(t, tree.IsLeaf())

	// PrettyPrint renders the whole tree in the |-- ASCII style. lop and ripple
	// are siblings at depth 2 with unspecified order, so accept either ordering.
	optionA := "|--marko\n   |--josh\n      |--lop\n      |--ripple"
	optionB := "|--marko\n   |--josh\n      |--ripple\n      |--lop"
	pretty := tree.PrettyPrint()
	assert.Contains(t, []string{optionA, optionB}, pretty)
}
