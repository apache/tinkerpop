/*
Licensed to the Apache Software Foundation (ASF) Under one
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
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTraversal(t *testing.T) {

	t.Run("Test clone traversal", func(t *testing.T) {
		g := cloneGraphTraversalSource(&Graph{}, NewGremlinLang(nil), nil)
		original := g.V().Out("created")
		clone := original.Clone().Out("knows")
		cloneClone := clone.Clone().Out("created")

		assert.Equal(t, 2, len(original.Bytecode.stepInstructions))
		assert.Equal(t, 3, len(clone.Bytecode.stepInstructions))
		assert.Equal(t, 4, len(cloneClone.Bytecode.stepInstructions))

		original.Has("person", "name", "marko")
		clone.V().Out()

		assert.Equal(t, 3, len(original.Bytecode.stepInstructions))
		assert.Equal(t, 5, len(clone.Bytecode.stepInstructions))
		assert.Equal(t, 4, len(cloneClone.Bytecode.stepInstructions))
	})

	t.Run("Test Iterate with empty removeConnection", func(t *testing.T) {
		g := NewGraphTraversalSource(&Graph{}, nil, NewBytecode(nil))

		promise := g.V().Count().Iterate()
		assert.NotNil(t, <-promise)
	})

	t.Run("Test traversal with bindings", func(t *testing.T) {
		g := cloneGraphTraversalSource(&Graph{}, NewGremlinLang(nil), nil)
		bytecode := g.V((&Bindings{}).Of("a", []int32{1, 2, 3})).
			Out((&Bindings{}).Of("b", "created")).
			Where(T__.In((&Bindings{}).Of("c", "created"), (&Bindings{}).Of("d", "knows")).
				Count().Is((&Bindings{}).Of("e", P.Gt(2)))).Bytecode
		assert.Equal(t, 5, len(bytecode.bindings))
		assert.Equal(t, []int32{1, 2, 3}, bytecode.bindings["a"])
		assert.Equal(t, "created", bytecode.bindings["b"])
		assert.Equal(t, "created", bytecode.bindings["c"])
		assert.Equal(t, "knows", bytecode.bindings["d"])
		assert.Equal(t, P.Gt(2), bytecode.bindings["e"])
		assert.Equal(t, &Binding{
			Key:   "b",
			Value: "created",
		}, bytecode.stepInstructions[1].arguments[0])
		assert.Equal(t, "binding[b=created]", bytecode.stepInstructions[1].arguments[0].(*Binding).String())
	})

	t.Run("Test Transaction commit", func(t *testing.T) {
		// Start a transaction traversal.
		remote := newConnection(t)
		g := Traversal_().With(remote)
		startCount := getCount(t, g)
		tx := g.Tx()

		// Except transaction to not be open until begin is called.
		assert.False(t, tx.IsOpen())
		gtx, _ := tx.Begin()
		assert.True(t, tx.IsOpen())

		addV(t, gtx, "lyndon")
		addV(t, gtx, "valentyn")
		assert.Equal(t, startCount, getCount(t, g))
		assert.Equal(t, startCount+2, getCount(t, gtx))

		// Commit the transaction, this should close it.
		// Our vertex count outside the transaction should be 2 + the start count.
		err := tx.Commit()
		assert.Nil(t, err)

		assert.False(t, tx.IsOpen())
		assert.Equal(t, startCount+2, getCount(t, g))

		dropGraphCheckCount(t, g)
		verifyGtxClosed(t, gtx)
	})

	t.Run("Test Transaction rollback", func(t *testing.T) {
		// Start a transaction traversal.
		remote := newConnection(t)
		g := Traversal_().With(remote)
		startCount := getCount(t, g)
		tx := g.Tx()

		// Except transaction to not be open until begin is called.
		assert.False(t, tx.IsOpen())
		gtx, _ := tx.Begin()
		assert.True(t, tx.IsOpen())

		addV(t, gtx, "lyndon")
		addV(t, gtx, "valentyn")
		assert.Equal(t, startCount, getCount(t, g))
		assert.Equal(t, startCount+2, getCount(t, gtx))

		// Rollback the transaction, this should close it.
		// Our vertex count outside the transaction should be the start count.
		err := tx.Rollback()
		assert.Nil(t, err)

		assert.False(t, tx.IsOpen())
		assert.Equal(t, startCount, getCount(t, g))

		dropGraphCheckCount(t, g)
		verifyGtxClosed(t, gtx)
	})

	t.Run("Test Transaction flows", func(t *testing.T) {
		// Start a transaction traversal.
		remote := newConnection(t)
		g := Traversal_().With(remote)
		tx := g.Tx()
		assert.False(t, tx.IsOpen())

		// Commit should return error when transaction not started
		err := tx.Commit()
		assert.NotNil(t, err)

		// Rollback should return error when transaction not started
		err = tx.Rollback()
		assert.NotNil(t, err)

		// Create transaction and verify it is open.
		gtx, err := tx.Begin()
		assert.Nil(t, err)
		assert.NotNil(t, gtx)
		assert.True(t, tx.IsOpen())

		// Can't open inner transaction.
		innerTx, err := gtx.Tx().Begin()
		assert.Nil(t, innerTx)
		assert.NotNil(t, err)

		// Commit this unused transaction and verify it is no longer open.
		err = tx.Commit()
		assert.Nil(t, err)
		assert.False(t, tx.IsOpen())

		// Create another transaction and verify it is open.
		gtx, err = tx.Begin()
		assert.Nil(t, err)
		assert.NotNil(t, gtx)
		assert.True(t, tx.IsOpen())

		// Rollback this unused transaction and verify it is no longer open.
		err = tx.Rollback()
		assert.Nil(t, err)
		assert.False(t, tx.IsOpen())
	})

	t.Run("Test multi commit Transaction", func(t *testing.T) {
		// Start a transaction traversal.
		remote := newConnection(t)
		g := Traversal_().With(remote)
		startCount := getCount(t, g)

		// Create two transactions.
		tx1 := g.Tx()
		tx2 := g.Tx()

		// Generate two GraphTraversalSource's for each transaction with begin.
		gtx1, _ := tx1.Begin()
		gtx2, _ := tx2.Begin()
		verifyTxState(t, true, tx1, tx2)

		// Add node to gtx1, which should be visible to gtx1, not gtx2.
		addNodeValidateTransactionState(t, g, gtx1, startCount, startCount, tx1, tx2)

		// Add node to gtx2, which should be visible to gtx2, not gtx1
		addNodeValidateTransactionState(t, g, gtx2, startCount, startCount, tx1, tx2)

		// Add node to gtx1, which should be visible to gtx1, not gtx2. Note previous node also added.
		addNodeValidateTransactionState(t, g, gtx1, startCount, startCount+1, tx1, tx2)

		tx1.Commit()
		verifyTxState(t, false, tx1)
		verifyTxState(t, true, tx2)
		assert.Equal(t, startCount+2, getCount(t, g))

		tx2.Commit()
		verifyTxState(t, false, tx1, tx2)
		assert.Equal(t, startCount+3, getCount(t, g))
	})

	t.Run("Test multi rollback Transaction", func(t *testing.T) {
		// Start a transaction traversal.
		remote := newConnection(t)
		g := Traversal_().With(remote)
		startCount := getCount(t, g)

		// Create two transactions.
		tx1 := g.Tx()
		tx2 := g.Tx()

		// Generate two GraphTraversalSource's for each transaction with begin.
		gtx1, _ := tx1.Begin()
		gtx2, _ := tx2.Begin()
		verifyTxState(t, true, tx1, tx2)

		// Add node to gtx1, which should be visible to gtx1, not gtx2.
		addNodeValidateTransactionState(t, g, gtx1, startCount, startCount, tx1, tx2)

		// Add node to gtx2, which should be visible to gtx2, not gtx1
		addNodeValidateTransactionState(t, g, gtx2, startCount, startCount, tx1, tx2)

		// Add node to gtx1, which should be visible to gtx1, not gtx2. Note previous node also added.
		addNodeValidateTransactionState(t, g, gtx1, startCount, startCount+1, tx1, tx2)

		tx1.Rollback()
		verifyTxState(t, false, tx1)
		verifyTxState(t, true, tx2)
		assert.Equal(t, startCount, getCount(t, g))

		tx2.Rollback()
		verifyTxState(t, false, tx1, tx2)
		assert.Equal(t, startCount, getCount(t, g))
	})

	t.Run("Test multi commit and rollback Transaction", func(t *testing.T) {
		// Start a transaction traversal.
		remote := newConnection(t)
		g := Traversal_().With(remote)
		startCount := getCount(t, g)

		// Create two transactions.
		tx1 := g.Tx()
		tx2 := g.Tx()

		// Generate two GraphTraversalSource's for each transaction with begin.
		gtx1, _ := tx1.Begin()
		gtx2, _ := tx2.Begin()
		verifyTxState(t, true, tx1, tx2)

		// Add node to gtx1, which should be visible to gtx1, not gtx2.
		addNodeValidateTransactionState(t, g, gtx1, startCount, startCount, tx1, tx2)

		// Add node to gtx2, which should be visible to gtx2, not gtx1
		addNodeValidateTransactionState(t, g, gtx2, startCount, startCount, tx1, tx2)

		// Add node to gtx1, which should be visible to gtx1, not gtx2. Note previous node also added.
		addNodeValidateTransactionState(t, g, gtx1, startCount, startCount+1, tx1, tx2)

		tx1.Commit()
		verifyTxState(t, false, tx1)
		verifyTxState(t, true, tx2)
		assert.Equal(t, startCount+2, getCount(t, g))

		tx2.Rollback()
		verifyTxState(t, false, tx1, tx2)
		assert.Equal(t, startCount+2, getCount(t, g))
	})

	t.Run("Test Transaction close", func(t *testing.T) {
		// Start a transaction traversal.
		remote := newConnection(t)
		g := Traversal_().With(remote)
		dropGraphCheckCount(t, g)

		// Create two transactions.
		tx1 := g.Tx()
		tx2 := g.Tx()

		// Generate two GraphTraversalSource's for each transaction with begin.
		gtx1, _ := tx1.Begin()
		gtx2, _ := tx2.Begin()
		verifyTxState(t, true, tx1, tx2)

		// Add stuff to both gtx.
		addNodeValidateTransactionState(t, g, gtx1, 0, 0, tx1, tx2)
		addNodeValidateTransactionState(t, g, gtx2, 0, 0, tx1, tx2)
		addNodeValidateTransactionState(t, g, gtx2, 0, 1, tx1, tx2)
		addNodeValidateTransactionState(t, g, gtx2, 0, 2, tx1, tx2)

		// someone gets lazy and doesn't commit/rollback and just calls close() - the graph
		// will decide how to treat the transaction, but for neo4j/gremlin server in this
		// test configuration it should rollback
		tx1.Close()
		tx2.Close()

		verifyGtxClosed(t, gtx1)
		verifyGtxClosed(t, gtx2)

		remote = newConnection(t)
		g = Traversal_().With(remote)
		assert.Equal(t, int32(0), getCount(t, g))
	})

	t.Run("Test Transaction close tx from parent", func(t *testing.T) {
		// Start a transaction traversal.
		remote := newConnection(t)
		g := Traversal_().With(remote)
		dropGraphCheckCount(t, g)

		// Create two transactions.
		tx1 := g.Tx()
		tx2 := g.Tx()

		// Generate two GraphTraversalSource's for each transaction with begin.
		gtx1, _ := tx1.Begin()
		gtx2, _ := tx2.Begin()
		verifyTxState(t, true, tx1, tx2)

		// Add stuff to both gtx.
		addNodeValidateTransactionState(t, g, gtx1, 0, 0, tx1, tx2)
		addNodeValidateTransactionState(t, g, gtx2, 0, 0, tx1, tx2)
		addNodeValidateTransactionState(t, g, gtx2, 0, 1, tx1, tx2)
		addNodeValidateTransactionState(t, g, gtx2, 0, 2, tx1, tx2)

		// someone gets lazy and doesn't commit/rollback and just calls Close() but on the parent
		// DriverRemoteConnection for all the session that were created via Tx() - the graph
		// will decide how to treat the transaction, but for neo4j/gremlin server in this
		// test configuration it should rollback.
		remote.Close()

		assert.False(t, tx1.IsOpen())
		assert.False(t, tx2.IsOpen())
		verifyGtxClosed(t, gtx1)
		verifyGtxClosed(t, gtx2)

		remote = newConnection(t)
		g = Traversal_().With(remote)
		assert.Equal(t, int32(0), getCount(t, g))
	})

	t.Run("Test commit if no transaction started", func(t *testing.T) {
		// Start a traversal.
		g := newWithOptionsConnection(t)

		// Create transactions
		tx := g.Tx()

		// try to commit
		err := tx.Commit()
		assert.Equal(t, "E1103: cannot commit a transaction that is not started", err.Error())
	})

	t.Run("Test rollback if no transaction started", func(t *testing.T) {
		// Start a traversal.
		g := newWithOptionsConnection(t)

		// Create transactions
		tx := g.Tx()

		// try to rollback
		err := tx.Rollback()
		assert.Equal(t, "E1102: cannot rollback a transaction that is not started", err.Error())
	})

	t.Run("Test commit if no transaction support for Graph", func(t *testing.T) {
		// Start a traversal.
		g := newWithOptionsConnection(t)

		// Create transactions
		tx := g.Tx()

		_, err := tx.Begin()
		assert.Nil(t, err)

		// try to commit
		err = tx.Commit()
		assert.True(t, strings.HasPrefix(err.Error(),
			"E0502: error in read loop, error message '{code:244 message:Graph does not support transactions"))
	})

	t.Run("Test WithOptions.Tokens WithOptions.None", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true))

		// Get GraphTraversalSource for WithOptions.
		g := newWithOptionsConnection(t)

		results, err := g.V().HasLabel("person").Has("name", "marko").ValueMap("name").With(WithOptions.Tokens, WithOptions.None).ToList()
		assert.Nil(t, err)
		assert.Equal(t, 1, len(results))
		name, ok := results[0].GetInterface().(map[interface{}]interface{})["name"]
		assert.True(t, ok)
		assert.NotNil(t, name)
		assert.Equal(t, 1, len(results[0].GetInterface().(map[interface{}]interface{})))
	})

	t.Run("Test WithOptions.Tokens WithOptions.Ids", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true))

		// Get GraphTraversalSource for WithOptions.
		g := newWithOptionsConnection(t)

		results, err := g.V().HasLabel("person").Has("name", "marko").Properties().ValueMap().With(WithOptions.Tokens, WithOptions.Ids).ToList()
		assert.Nil(t, err)
		assert.True(t, len(results) > 0)

		// Expect each result to contain a id. No additional items.
		for _, result := range results {
			key, ok := result.GetInterface().(map[interface{}]interface{})["id"]
			assert.True(t, ok)
			assert.NotNil(t, key)

			// Size should be 1.
			assert.Equal(t, 1, len(result.GetInterface().(map[interface{}]interface{})))
		}
	})

	t.Run("Test WithOptions.Tokens WithOptions.Keys", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true))

		// Get GraphTraversalSource for WithOptions.
		g := newWithOptionsConnection(t)

		results, err := g.V().HasLabel("person").Has("name", "marko").Properties().ValueMap().With(WithOptions.Tokens, WithOptions.Keys).ToList()
		assert.Nil(t, err)
		assert.True(t, len(results) > 0)

		// Expect each result to contain a key. No additional items.
		for _, result := range results {
			key, ok := result.GetInterface().(map[interface{}]interface{})["key"]
			assert.True(t, ok)
			assert.NotNil(t, key)

			// Size should be 1.
			assert.Equal(t, 1, len(result.GetInterface().(map[interface{}]interface{})))
		}
	})

	t.Run("Test WithOptions.Tokens WithOptions.Values", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true))

		// Get GraphTraversalSource for WithOptions.
		g := newWithOptionsConnection(t)

		results, err := g.V().HasLabel("person").Has("name", "marko").Properties().ValueMap().With(WithOptions.Tokens, WithOptions.Values).ToList()
		assert.Nil(t, err)
		assert.True(t, len(results) > 0)

		// Expect each result to contain a value. No additional items.
		for _, result := range results {
			key, ok := result.GetInterface().(map[interface{}]interface{})["value"]
			assert.True(t, ok)
			assert.NotNil(t, key)

			// Size should be 1.
			assert.Equal(t, 1, len(result.GetInterface().(map[interface{}]interface{})))
		}
	})

	t.Run("Test WithOptions.Tokens WithOptions.All", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true))

		// Get GraphTraversalSource for WithOptions.
		g := newWithOptionsConnection(t)

		results, err := g.V().HasLabel("person").Has("name", "marko").Properties().ValueMap().With(WithOptions.Tokens, WithOptions.All).ToList()
		assert.Nil(t, err)
		assert.True(t, len(results) > 0)

		// Expect each result to contain an id, key, and value. No additional items.
		for _, result := range results {
			id, ok := result.GetInterface().(map[interface{}]interface{})["id"]
			assert.True(t, ok)
			assert.NotNil(t, id)

			key, ok := result.GetInterface().(map[interface{}]interface{})["key"]
			assert.True(t, ok)
			assert.NotNil(t, key)

			value, ok := result.GetInterface().(map[interface{}]interface{})["value"]
			assert.True(t, ok)
			assert.NotNil(t, value)

			// Size should be 3.
			assert.Equal(t, 3, len(result.GetInterface().(map[interface{}]interface{})))
		}
	})

	t.Run("Test WithOptions.Indexer WithOptions.List", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true))

		// Get GraphTraversalSource for WithOptions.
		g := newWithOptionsConnection(t)

		// We expect our ResultSet to be a single Result that contains a list (array or slice).
		results, err := g.V().HasLabel("person").Values("name").Fold().Index().With(WithOptions.Indexer, WithOptions.List).ToList()
		assert.Nil(t, err)
		assert.Equal(t, 1, len(results))
		assert.True(t, results[0].GetType().Kind() == reflect.Array || results[0].GetType().Kind() == reflect.Slice)
	})

	t.Run("Test WithOptions.Indexer WithOptions.Map", func(t *testing.T) {
		skipTestsIfNotEnabled(t, integrationTestSuiteName, getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true))

		// Get GraphTraversalSource for WithOptions.
		g := newWithOptionsConnection(t)

		// We expect our ResultSet to be a single Result that contains a map.
		results, err := g.V().HasLabel("person").Values("name").Fold().Index().With(WithOptions.Indexer, WithOptions.Map).ToList()
		assert.Nil(t, err)
		assert.Equal(t, 1, len(results))
		assert.True(t, results[0].GetType().Kind() == reflect.Map)

	})
}

func newWithOptionsConnection(t *testing.T) *GraphTraversalSource {
	// No authentication integration test with graphs loaded and alias configured server
	testNoAuthWithAliasUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	testNoAuthWithAliasAuthInfo := &AuthInfo{}
	testNoAuthWithAliasTlsConfig := &tls.Config{}

	remote, err := NewDriverRemoteConnection(testNoAuthWithAliasUrl,
		func(settings *DriverRemoteConnectionSettings) {
			settings.TlsConfig = testNoAuthWithAliasTlsConfig
			settings.AuthInfo = testNoAuthWithAliasAuthInfo
			settings.TraversalSource = "gmodern"
		})
	assert.Nil(t, err)
	assert.NotNil(t, remote)
	return Traversal_().With(remote)
}

func newConnection(t *testing.T) *DriverRemoteConnection {
	testNoAuthWithAliasUrl := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	testNoAuthWithAliasAuthInfo := &AuthInfo{}
	testNoAuthWithAliasTlsConfig := &tls.Config{}

	remote, err := NewDriverRemoteConnection(testNoAuthWithAliasUrl,
		func(settings *DriverRemoteConnectionSettings) {
			settings.TlsConfig = testNoAuthWithAliasTlsConfig
			settings.AuthInfo = testNoAuthWithAliasAuthInfo
			settings.TraversalSource = "gtx"
		})
	assert.Nil(t, err)
	assert.NotNil(t, remote)
	return remote
}

func addNodeValidateTransactionState(t *testing.T, g, gAddTo *GraphTraversalSource,
	gStartCount, gAddToStartCount int32, txVerifyList ...*Transaction) {
	// Add a single node to gAddTo, but not g.
	// Check that vertex count in g is gStartCount and vertex count in gAddTo is gAddToStartCount + 1.
	addV(t, gAddTo, "lyndon")
	assert.Equal(t, gAddToStartCount+1, getCount(t, gAddTo))
	assert.Equal(t, gStartCount, getCount(t, g))
	verifyTxState(t, true, txVerifyList...)
}

func verifyTxState(t *testing.T, expected bool, gtxList ...*Transaction) {
	for _, tx := range gtxList {
		assert.Equal(t, expected, tx.IsOpen())
	}
}

func addV(t *testing.T, g *GraphTraversalSource, name string) {
	promise := g.AddV("person").Property("name", name).Iterate()
	assert.Nil(t, <-promise)
}

func dropGraphCheckCount(t *testing.T, g *GraphTraversalSource) {
	dropGraph(t, g)
	assert.Equal(t, int32(0), getCount(t, g))
}

func verifyGtxClosed(t *testing.T, gtx *GraphTraversalSource) {
	// Attempt to add an additional vertex to the transaction. This should return an error since it
	// has been closed.
	promise := gtx.AddV("failure").Iterate()
	assert.NotNil(t, <-promise)
}

func getCount(t *testing.T, g *GraphTraversalSource) int32 {
	count, err := g.V().Count().ToList()
	assert.Nil(t, err)
	assert.NotNil(t, count)
	assert.Equal(t, 1, len(count))
	val, err := count[0].GetInt32()
	assert.Nil(t, err)
	return val
}
