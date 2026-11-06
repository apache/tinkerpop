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

// transactionRemoteConnection is a remote connection that attaches a
// transactionId to all requests, binding them to a server-side transaction.
type transactionRemoteConnection struct {
	transaction *Transaction
}

func (t *transactionRemoteConnection) Close() {
	// No-op: the transaction lifecycle is managed by Transaction.Close/Commit/Rollback
}

func (t *transactionRemoteConnection) submitGremlinLang(gremlinLang *GremlinLang) (ResultSet, error) {
	if !t.transaction.isOpen {
		return nil, newError(err1106TransactionNotOpenError)
	}
	builder := new(RequestOptionsBuilder)
	builder.SetTransactionId(t.transaction.transactionId)
	return t.transaction.client.submitGremlinLangWithBuilder(gremlinLang, builder)
}
