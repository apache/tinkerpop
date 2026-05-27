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

	_, err = tx.Begin()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "E1101")
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

func TestTransactionBeginFromGtxTxThrows(t *testing.T) {
	client := newTxClient(t)
	defer client.Close()

	remote := newTxRemoteConnection(t)
	defer remote.Close()
	g := Traversal_().With(remote)

	tx := g.Tx()
	gtx, err := tx.Begin()
	assert.Nil(t, err)

	sameTx := gtx.Tx()
	_, err = sameTx.Begin()
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "E1101")

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
