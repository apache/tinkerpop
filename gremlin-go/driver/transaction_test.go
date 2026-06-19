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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newTxRemoteConnection(t *testing.T) *DriverRemoteConnection {
	url := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	remote, err := NewDriverRemoteConnection(url,
		func(settings *DriverRemoteConnectionSettings) {
			settings.TlsConfig = &tls.Config{}
			settings.TraversalSource = "gtx"
		})
	assert.Nil(t, err)
	assert.NotNil(t, remote)
	return remote
}

func newTxClient(t *testing.T) *Client {
	url := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	client, err := NewClient(url, func(settings *ClientSettings) {
		settings.TlsConfig = &tls.Config{}
		settings.TraversalSource = "gtx"
	})
	assert.Nil(t, err)
	assert.NotNil(t, client)
	return client
}

// dropTxGraph removes all vertices from the transactional graph to prevent cross-test contamination.
func dropTxGraph(t *testing.T, client *Client) {
	rs, err := client.Submit("g.V().drop()")
	assert.Nil(t, err)
	_, err = rs.All()
	assert.Nil(t, err)
}

func getTxCount(t *testing.T, client *Client, label string) int64 {
	rs, err := client.Submit("g.V().hasLabel('" + label + "').count()")
	assert.Nil(t, err)
	results, err := rs.All()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	val, err := results[0].GetInt64()
	assert.Nil(t, err)
	return val
}

func TestTransactionCommit(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	tx := client.Transact()
	_, err := tx.Begin()
	assert.Nil(t, err)
	assert.True(t, tx.IsOpen())

	_, err = tx.Submit("g.addV('person').property('name','alice')")
	assert.Nil(t, err)

	// Uncommitted data not visible outside the transaction
	assert.Equal(t, int64(0), getTxCount(t, client, "person"))

	err = tx.Commit()
	assert.Nil(t, err)
	assert.False(t, tx.IsOpen())

	// Committed data visible
	assert.Equal(t, int64(1), getTxCount(t, client, "person"))
}

func TestTransactionRollback(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	tx := client.Transact()
	_, err := tx.Begin()
	assert.Nil(t, err)

	_, err = tx.Submit("g.addV('person').property('name','bob')")
	assert.Nil(t, err)

	err = tx.Rollback()
	assert.Nil(t, err)
	assert.False(t, tx.IsOpen())

	// Data discarded
	assert.Equal(t, int64(0), getTxCount(t, client, "person"))
}

func TestTransactionIntraConsistency(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	tx := client.Transact()
	_, err := tx.Begin()
	assert.Nil(t, err)

	_, err = tx.Submit("g.addV('test').property('name','A')")
	assert.Nil(t, err)

	// Read-your-own-writes
	rs, err := tx.Submit("g.V().hasLabel('test').count()")
	assert.Nil(t, err)
	results, err := rs.All()
	assert.Nil(t, err)
	val, err := results[0].GetInt64()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), val)

	_, err = tx.Submit("g.addV('test').property('name','B')")
	assert.Nil(t, err)

	err = tx.Commit()
	assert.Nil(t, err)

	assert.Equal(t, int64(2), getTxCount(t, client, "test"))
}

func TestTransactionSubmitAfterCommit(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()

	tx := client.Transact()
	_, err := tx.Begin()
	assert.Nil(t, err)

	_, err = tx.Submit("g.addV()")
	assert.Nil(t, err)

	err = tx.Commit()
	assert.Nil(t, err)

	_, err = tx.Submit("g.V().count()")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "transaction is not open")
}

func TestTransactionSubmitAfterRollback(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()

	tx := client.Transact()
	_, err := tx.Begin()
	assert.Nil(t, err)

	_, err = tx.Submit("g.addV()")
	assert.Nil(t, err)

	err = tx.Rollback()
	assert.Nil(t, err)

	_, err = tx.Submit("g.V().count()")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "transaction is not open")
}

func TestTransactionDoubleBegin(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()

	tx := client.Transact()
	_, err := tx.Begin()
	assert.Nil(t, err)
	txId := tx.TransactionId()

	// Begin() while already open is idempotent: it does not error and does not start a new
	// server-side transaction (the transactionId is unchanged)
	_, err = tx.Begin()
	assert.Nil(t, err)
	assert.True(t, tx.IsOpen())
	assert.Equal(t, txId, tx.TransactionId())

	tx.Rollback()
}

func TestTransactionCommitWhenNotOpen(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()

	tx := client.Transact()
	err := tx.Commit()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "E1103")
}

func TestTransactionRollbackWhenNotOpen(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()

	tx := client.Transact()
	err := tx.Rollback()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "E1102")
}

func TestTransactionIdBeforeAndAfterBegin(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()

	tx := client.Transact()
	assert.Equal(t, "", tx.TransactionId())

	_, err := tx.Begin()
	assert.Nil(t, err)
	assert.NotEqual(t, "", tx.TransactionId())
	assert.True(t, len(tx.TransactionId()) > 0)

	tx.Rollback()
}

func TestTransactionRollbackOnCloseByDefault(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	tx := client.Transact()
	_, err := tx.Begin()
	assert.Nil(t, err)

	_, err = tx.Submit("g.addV('person').property('name','close_test')")
	assert.Nil(t, err)

	err = tx.Close()
	assert.Nil(t, err)
	assert.False(t, tx.IsOpen())

	// Data should NOT persist (rollback is default)
	assert.Equal(t, int64(0), getTxCount(t, client, "person"))
}

func TestTransactionIsolateConcurrent(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	tx1 := client.Transact()
	_, err := tx1.Begin()
	assert.Nil(t, err)

	tx2 := client.Transact()
	_, err = tx2.Begin()
	assert.Nil(t, err)

	_, err = tx1.Submit("g.addV('tx1')")
	assert.Nil(t, err)
	_, err = tx2.Submit("g.addV('tx2')")
	assert.Nil(t, err)

	// tx1 should not see tx2's data
	rs, err := tx1.Submit("g.V().hasLabel('tx2').count()")
	assert.Nil(t, err)
	results, _ := rs.All()
	val, _ := results[0].GetInt64()
	assert.Equal(t, int64(0), val)

	err = tx1.Commit()
	assert.Nil(t, err)
	err = tx2.Commit()
	assert.Nil(t, err)

	assert.Equal(t, int64(1), getTxCount(t, client, "tx1"))
	assert.Equal(t, int64(1), getTxCount(t, client, "tx2"))
}

func TestTransactionSequentialStress(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	numTransactions := 50
	for i := 0; i < numTransactions; i++ {
		tx := client.Transact()
		_, err := tx.Begin()
		assert.Nil(t, err)
		_, err = tx.Submit("g.addV('stress')")
		assert.Nil(t, err)
		err = tx.Commit()
		assert.Nil(t, err)
	}

	assert.Equal(t, int64(numTransactions), getTxCount(t, client, "stress"))
}

func TestTransactionKeepOpenAfterTraversalError(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	tx := client.Transact()
	_, err := tx.Begin()
	assert.Nil(t, err)

	_, err = tx.Submit("g.addV('good_vertex')")
	assert.Nil(t, err)

	// Submit a bad traversal - GraphBinary errors surface when draining the ResultSet
	// because the server responds with HTTP 200 + error in the body
	rs, err := tx.Submit("g.V().fail()")
	if err == nil {
		_, err = rs.All()
	}
	assert.NotNil(t, err)

	// Transaction should still be open
	assert.True(t, tx.IsOpen())
	err = tx.Rollback()
	assert.Nil(t, err)

	assert.Equal(t, int64(0), getTxCount(t, client, "good_vertex"))
}

func TestTransactionWithTraversalAPI(t *testing.T) {
	remote := newTxRemoteConnection(t)
	defer remote.Close()

	g := Traversal_().With(remote)

	tx := g.Tx()
	gtx, err := tx.Begin()
	assert.Nil(t, err)

	promise := gtx.AddV("val").Iterate()
	assert.Nil(t, <-promise)

	err = tx.Commit()
	assert.Nil(t, err)

	count, err := g.V().HasLabel("val").Count().ToList()
	assert.Nil(t, err)
	val, err := count[0].GetInt64()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), val)
}

func TestTransactionRejectBeginOnNonTransactionalGraph(t *testing.T) {
	url := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	client, err := NewClient(url, func(settings *ClientSettings) {
		settings.TlsConfig = &tls.Config{}
		settings.TraversalSource = "gclassic"
	})
	assert.Nil(t, err)
	defer client.Close()

	tx := client.Transact()
	_, err = tx.Begin()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Graph does not support transactions")
}

func TestTransactionCleanUpOnBeginFailure(t *testing.T) {
	url := getEnvOrDefaultString("GREMLIN_SERVER_URL", noAuthUrl)
	client, err := NewClient(url, func(settings *ClientSettings) {
		settings.TlsConfig = &tls.Config{}
		settings.TraversalSource = "gclassic"
	})
	assert.Nil(t, err)
	defer client.Close()

	tx := client.Transact()
	_, err = tx.Begin()
	assert.NotNil(t, err)

	// Transaction should not be open after failed begin
	assert.False(t, tx.IsOpen())
	assert.Equal(t, "", tx.TransactionId())

	// Cannot begin again
	_, err = tx.Begin()
	assert.NotNil(t, err)
}

func TestTransactionReturnsSameTxFromGtxTx(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	remote := newTxRemoteConnection(t)
	defer remote.Close()
	g := Traversal_().With(remote)

	tx := g.Tx()
	gtx, err := tx.Begin()
	assert.Nil(t, err)

	sameTx := gtx.Tx()
	assert.Equal(t, tx, sameTx)

	err = tx.Rollback()
	assert.Nil(t, err)
}

func TestTransactionBeginFromGtxTxIsIdempotent(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()

	remote := newTxRemoteConnection(t)
	defer remote.Close()
	g := Traversal_().With(remote)

	tx := g.Tx()
	gtx, err := tx.Begin()
	assert.Nil(t, err)
	txId := tx.TransactionId()

	// Begin() on the same (already open) transaction obtained via gtx.Tx() is idempotent: it does
	// not start a new server-side transaction, so it stays bound to the same transaction id
	sameTx := gtx.Tx()
	_, err = sameTx.Begin()
	assert.Nil(t, err)
	assert.True(t, sameTx.IsOpen())
	assert.Equal(t, txId, sameTx.TransactionId())

	tx.Rollback()
}

func TestTransactionCommitViaGtxTx(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	remote := newTxRemoteConnection(t)
	defer remote.Close()
	g := Traversal_().With(remote)

	tx := g.Tx()
	gtx, err := tx.Begin()
	assert.Nil(t, err)

	promise := gtx.AddV("gtx_commit_test").Iterate()
	assert.Nil(t, <-promise)

	err = gtx.Tx().Commit()
	assert.Nil(t, err)

	assert.Equal(t, int64(1), getTxCount(t, client, "gtx_commit_test"))
}

func TestTransactionDoubleCommit(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	tx := client.Transact()
	_, err := tx.Begin()
	assert.Nil(t, err)
	err = tx.Commit()
	assert.Nil(t, err)

	err = tx.Commit()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "E1103")
}

func TestTransactionDoubleRollback(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	tx := client.Transact()
	_, err := tx.Begin()
	assert.Nil(t, err)
	err = tx.Rollback()
	assert.Nil(t, err)

	err = tx.Rollback()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "E1102")
}

func TestTransactionDoubleClose(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	tx := client.Transact()
	_, err := tx.Begin()
	assert.Nil(t, err)
	err = tx.Close()
	assert.Nil(t, err)
	assert.False(t, tx.IsOpen())

	// Close() is idempotent: closing an already-closed transaction is a safe no-op (no error)
	err = tx.Close()
	assert.Nil(t, err)
	assert.False(t, tx.IsOpen())
}

func TestTransactionIsolateFromNonTx(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	tx := client.Transact()
	_, err := tx.Begin()
	assert.Nil(t, err)

	_, err = tx.Submit("g.addV('tx_data')")
	assert.Nil(t, err)

	// Non-transactional read should not see uncommitted data
	assert.Equal(t, int64(0), getTxCount(t, client, "tx_data"))

	err = tx.Commit()
	assert.Nil(t, err)

	assert.Equal(t, int64(1), getTxCount(t, client, "tx_data"))
}

func TestTransactionBeginAfterCommit(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	tx := client.Transact()
	_, err := tx.Begin()
	assert.Nil(t, err)
	err = tx.Commit()
	assert.Nil(t, err)

	_, err = tx.Begin()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "E1101")
}

func TestTransactionBeginAfterRollback(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	tx := client.Transact()
	_, err := tx.Begin()
	assert.Nil(t, err)
	err = tx.Rollback()
	assert.Nil(t, err)

	_, err = tx.Begin()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "E1101")
}

func TestTransactionRollbackOnClientClose(t *testing.T) {
	client := newTxClient(t)
	dropTxGraph(t, client)

	tx := client.Transact()
	_, err := tx.Begin()
	assert.Nil(t, err)

	_, err = tx.Submit("g.addV('client_close_test')")
	assert.Nil(t, err)

	client.Close()

	assert.False(t, tx.IsOpen())

	client2 := newTxClient(t)
	defer client2.Close()
	assert.Equal(t, int64(0), getTxCount(t, client2, "client_close_test"))
}

func TestTransactionRollbackOnDrcClose(t *testing.T) {
	remote := newTxRemoteConnection(t)

	g := Traversal_().With(remote)

	tx := g.Tx()
	gtx, err := tx.Begin()
	assert.Nil(t, err)

	promise := gtx.AddV("drc_close_test").Iterate()
	assert.Nil(t, <-promise)

	remote.Close()

	assert.False(t, tx.IsOpen())

	client2 := newTxClient(t)
	defer client2.Close()
	dropTxGraph(t, client2)
	assert.Equal(t, int64(0), getTxCount(t, client2, "drc_close_test"))
}

func TestTransactionMultiRollback(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	tx1 := client.Transact()
	_, err := tx1.Begin()
	assert.Nil(t, err)
	tx2 := client.Transact()
	_, err = tx2.Begin()
	assert.Nil(t, err)

	_, err = tx1.Submit("g.addV('multi_rb1')")
	assert.Nil(t, err)
	_, err = tx2.Submit("g.addV('multi_rb2')")
	assert.Nil(t, err)

	err = tx1.Rollback()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), getTxCount(t, client, "multi_rb1"))

	err = tx2.Rollback()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), getTxCount(t, client, "multi_rb2"))
}

func TestTransactionMultiCommitAndRollback(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	tx1 := client.Transact()
	_, err := tx1.Begin()
	assert.Nil(t, err)
	tx2 := client.Transact()
	_, err = tx2.Begin()
	assert.Nil(t, err)

	_, err = tx1.Submit("g.addV('multi_cr1')")
	assert.Nil(t, err)
	_, err = tx2.Submit("g.addV('multi_cr2')")
	assert.Nil(t, err)

	err = tx1.Commit()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), getTxCount(t, client, "multi_cr1"))

	err = tx2.Rollback()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), getTxCount(t, client, "multi_cr2"))
}

func TestTransactionExecuteInTxCommitsOnSuccess(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	remote := newTxRemoteConnection(t)
	defer remote.Close()
	g := Traversal_().With(remote)

	err := g.ExecuteInTx(func(gtx *GraphTraversalSource) error {
		promise := gtx.AddV("person").Iterate()
		return <-promise
	})
	assert.Nil(t, err)

	// Committed data is visible outside the transaction.
	assert.Equal(t, int64(1), getTxCount(t, client, "person"))
}

func TestTransactionExecuteInTxRollsBackAndRethrowsOnBodyError(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	remote := newTxRemoteConnection(t)
	defer remote.Close()
	g := Traversal_().With(remote)

	sentinel := errors.New("intentional body failure")

	err := g.ExecuteInTx(func(gtx *GraphTraversalSource) error {
		// Add a vertex, then fail: the add must be rolled back.
		promise := gtx.AddV("person").Iterate()
		assert.Nil(t, <-promise)
		return sentinel
	})

	// (i) the exact original error is returned unchanged.
	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, sentinel))
	assert.Equal(t, sentinel, err)

	// (ii) the vertex was NOT persisted (rollback happened).
	assert.Equal(t, int64(0), getTxCount(t, client, "person"))
}

// EvaluateInTx returns the value computed by the body as interface{}, matching
// the driver's untyped result API; the caller type-asserts it.
func TestTransactionEvaluateInTxReturnsValue(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	remote := newTxRemoteConnection(t)
	defer remote.Close()
	g := Traversal_().With(remote)

	v, err := g.EvaluateInTx(func(gtx *GraphTraversalSource) (interface{}, error) {
		// Add two vertices and return the in-transaction count.
		promise := gtx.AddV("widget").Iterate()
		if pErr := <-promise; pErr != nil {
			return nil, pErr
		}
		promise = gtx.AddV("widget").Iterate()
		if pErr := <-promise; pErr != nil {
			return nil, pErr
		}
		counts, cErr := gtx.V().HasLabel("widget").Count().ToList()
		if cErr != nil {
			return nil, cErr
		}
		count, cErr := counts[0].GetInt64()
		if cErr != nil {
			return nil, cErr
		}
		return count, nil
	})
	assert.Nil(t, err)
	// the body returned an int64; the caller type-asserts the interface{} result
	assert.Equal(t, int64(2), v)

	// The committed value matches what was returned.
	assert.Equal(t, int64(2), getTxCount(t, client, "widget"))
}

// Opening a SECOND transaction from inside the body must error. gtx.Tx() itself
// legitimately returns the SAME transaction (it must not error - that is the
// commit path), and a nested Begin() on it is idempotent: it does not error and
// reuses the same already-open transaction.
func TestTransactionExecuteInTxBeginIsIdempotent(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	remote := newTxRemoteConnection(t)
	defer remote.Close()
	g := Traversal_().With(remote)

	err := g.ExecuteInTx(func(gtx *GraphTraversalSource) error {
		// gtx.Tx() returns the bound, already-open transaction (must NOT error).
		tx := gtx.Tx()
		assert.True(t, tx.IsOpen())
		txId := tx.TransactionId()

		// gtx.Tx() called again returns that same transaction handle.
		assert.Equal(t, tx, gtx.Tx())

		// Begin() on the already-open transaction is idempotent: no error, no new server-side
		// transaction (the transaction id is unchanged).
		gtx2, beginErr := tx.Begin()
		assert.Nil(t, beginErr)
		assert.NotNil(t, gtx2)
		assert.Equal(t, txId, gtx2.Tx().TransactionId())

		return nil
	})

	assert.Nil(t, err)
}

// Verifies that a failure of the wrapper's automatic Commit() is surfaced from
// ExecuteInTx. To drive a deterministic, no-mock commit failure, the body
// succeeds but the transaction is rolled back from a SEPARATE connection (by its
// transactionId) before the body returns; the wrapper's Commit() is then a real
// commit RPC that the server rejects with "Transaction not found".
func TestTransactionExecuteInTxPropagatesCommitFailure(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	// a second connection used to kill the transaction out-of-band
	sideClient := newTxClient(t)
	defer sideClient.Close()

	remote := newTxRemoteConnection(t)
	defer remote.Close()
	g := Traversal_().With(remote)

	err := g.ExecuteInTx(func(gtx *GraphTraversalSource) error {
		if pErr := <-gtx.AddV("commit_fail").Iterate(); pErr != nil {
			return pErr
		}
		// Roll the transaction back from a separate connection, targeting it by its
		// transactionId. The body still returns nil, so the wrapper proceeds to commit -
		// which now fails server-side because the transaction no longer exists.
		txId := gtx.Tx().TransactionId()
		_, rbErr := sideClient.SubmitWithOptions("g.tx().rollback()",
			new(RequestOptionsBuilder).SetTransactionId(txId).Create())
		return rbErr
	})

	// the commit failure is the error surfaced to the caller
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Transaction not found")

	// data was not persisted (the transaction was rolled back out-of-band)
	assert.Equal(t, int64(0), getTxCount(t, client, "commit_fail"))
}

func TestTransactionExecuteInTxRollsBackAndRepanicsOnPanic(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()
	dropTxGraph(t, client)

	remote := newTxRemoteConnection(t)
	defer remote.Close()
	g := Traversal_().With(remote)

	func() {
		defer func() {
			r := recover()
			// The panic must propagate out of ExecuteInTx (not be swallowed).
			assert.NotNil(t, r)
			assert.Equal(t, "intentional body panic", r)
		}()

		_ = g.ExecuteInTx(func(gtx *GraphTraversalSource) error {
			promise := gtx.AddV("panic_vertex").Iterate()
			assert.Nil(t, <-promise)
			panic("intentional body panic")
		})
		// Unreachable: ExecuteInTx must re-panic.
		assert.Fail(t, "expected ExecuteInTx to re-panic")
	}()

	// The transaction was rolled back during panic handling: nothing persisted.
	assert.Equal(t, int64(0), getTxCount(t, client, "panic_vertex"))
}
