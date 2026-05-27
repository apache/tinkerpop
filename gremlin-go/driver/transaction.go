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
	"fmt"
	"sync"
)

// Transaction controls an explicit remote transaction. A thin wrapper around
// a Client that adds transaction lifecycle (begin/commit/rollback/close) and
// attaches a transactionId to every request.
//
// Created via Client.Transact() or GraphTraversalSource.Tx(). The traversal
// source (g alias) is inherited from the Client and cannot be changed.
//
// Transactions are short-lived and single-use. After commit or rollback, the
// transaction ID is invalid and the object cannot be reused.
//
// This struct is NOT safe for concurrent semantic use. The mutex serializes
// requests if a user accidentally shares the Transaction across goroutines.
type Transaction struct {
	client        *Client
	transactionId string
	isOpen        bool
	failed        bool
	mutex         sync.Mutex
}

func (t *Transaction) Begin() (*GraphTraversalSource, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.isOpen || t.failed {
		return nil, newError(err1101TransactionRepeatedOpenError)
	}

	// Submit g.tx().begin() via the Client to obtain a server-generated transactionId
	rs, err := t.client.SubmitWithOptions("g.tx().begin()",
		RequestOptions{})
	if err != nil {
		t.failed = true
		return nil, newError(err1105TransactionBeginFailedError, err)
	}

	results, err := rs.All()
	if err != nil {
		t.failed = true
		return nil, newError(err1105TransactionBeginFailedError, err)
	}

	txId, err := extractTransactionId(results)
	if err != nil {
		t.failed = true
		return nil, err
	}

	t.transactionId = txId
	t.isOpen = true
	t.client.trackTransaction(t)

	// Create a transaction-bound remote connection that injects transactionId
	txDRC := &transactionRemoteConnection{
		transaction: t,
	}
	gts := &GraphTraversalSource{
		graph:            nil,
		gremlinLang:      NewGremlinLang(nil),
		remoteConnection: txDRC,
	}
	return gts, nil
}

func (t *Transaction) Commit() error {
	return t.closeTransaction("g.tx().commit()", err1103TransactionCommitNotOpenedError)
}

func (t *Transaction) Rollback() error {
	return t.closeTransaction("g.tx().rollback()", err1102TransactionRollbackNotOpenedError)
}

// Close rolls back the transaction if still open. This is the safe default:
// partial work is discarded rather than accidentally persisted.
func (t *Transaction) Close() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.isOpen {
		return nil
	}

	return t.closeTransactionLocked("g.tx().rollback()")
}

func (t *Transaction) IsOpen() bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.isOpen
}

// TransactionId returns the server-generated transaction ID, or empty string if not yet begun.
func (t *Transaction) TransactionId() string {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.transactionId
}

func (t *Transaction) closeTransaction(script string, notOpenErr errorCode) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.isOpen {
		return newError(notOpenErr)
	}

	return t.closeTransactionLocked(script)
}

func (t *Transaction) closeTransactionLocked(script string) error {
	opts := new(RequestOptionsBuilder)
	opts.SetTransactionId(t.transactionId)
	rs, err := t.client.SubmitWithOptions(script, opts.Create())
	if err != nil {
		return err
	}
	// Drain the result set to ensure the response is fully consumed
	_, err = rs.All()
	if err != nil {
		return err
	}
	// Only mark closed after server confirms success
	t.isOpen = false
	t.failed = true // Terminal state: transaction cannot be reused
	t.client.untrackTransaction(t)
	return nil
}

// Submit sends a plain gremlin-lang string within this transaction.
// This is the driver-level API for users who don't want to use the Traversal API.
// The transactionId is automatically attached.
func (t *Transaction) Submit(gremlin string, options ...RequestOptions) (ResultSet, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.isOpen {
		return nil, newError(err1106TransactionNotOpenError)
	}

	var opts RequestOptions
	if len(options) > 0 {
		opts = options[0]
	}
	opts.transactionId = t.transactionId

	return t.client.SubmitWithOptions(gremlin, opts)
}

func extractTransactionId(results []*Result) (string, error) {
	if len(results) == 0 {
		return "", fmt.Errorf("server did not return transaction ID")
	}

	resultVal := results[0].GetInterface()
	resultMap, ok := resultVal.(map[interface{}]interface{})
	if !ok {
		return "", fmt.Errorf("server did not return transaction ID in expected format")
	}

	txId, ok := resultMap["transactionId"]
	if !ok {
		return "", fmt.Errorf("server did not return transaction ID in expected format")
	}

	s, ok := txId.(string)
	if !ok || s == "" {
		return "", fmt.Errorf("server did not return transaction ID in expected format")
	}

	return s, nil
}
