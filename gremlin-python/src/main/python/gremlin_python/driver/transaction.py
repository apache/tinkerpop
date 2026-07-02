#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import logging

from gremlin_python.driver.remote_connection import RemoteConnection, RemoteTraversal

log = logging.getLogger("gremlinpython")


class Transaction:
    """Controls an explicit remote transaction. A thin wrapper around a Client
    that adds transaction lifecycle (begin/commit/rollback/close) and attaches
    a transactionId to every request.

    Created via Client.transact() or g.tx(). The traversal source (g alias)
    is inherited from the Client and cannot be changed.

    Transactions are short-lived and single-use. After commit or rollback,
    the transaction ID is invalid and the object cannot be reused.

    This class is NOT thread-safe.
    """

    def __init__(self, client):
        self._client = client
        self._transaction_id = None
        self._is_open = False
        self._failed = False

    def begin(self):
        """Starts the transaction and returns a transaction-bound GraphTraversalSource.

        The returned GTS can be used to submit traversals within this transaction.
        Users of the driver-level API (client.transact()) may ignore the return
        value and use submit() directly instead.

        begin() is idempotent: calling it while a transaction is already open does not
        send a second begin to the server and does not raise - it reuses the existing
        transaction ID and returns a source bound to the same transaction. A transaction
        is single-use, so calling begin() after it has been closed raises.
        """
        if self._failed:
            raise Exception("Transaction is closed and cannot be reused; begin a new transaction")

        # idempotent: if a transaction is already open, reuse the existing transactionId without
        # sending a second begin to the server, and return a source bound to the same transaction
        if not self._is_open:
            try:
                result = self._client.submit("g.tx().begin()")
                results = result.all().result()
            except Exception:
                self._failed = True
                raise

            if not results:
                self._failed = True
                raise Exception("Server did not return transaction ID")

            result_map = results[0]
            if isinstance(result_map, dict):
                self._transaction_id = result_map.get('transactionId')
            else:
                self._failed = True
                raise Exception("Server did not return transaction ID in expected format")

            if not self._transaction_id:
                self._failed = True
                raise Exception("Server returned empty transaction ID")

            self._is_open = True
            self._client.track_transaction(self)

        # Return a GraphTraversalSource bound to this transaction via
        # TransactionRemoteConnection. Inline imports avoid circular dependencies
        # between driver/ and process/ packages.
        from gremlin_python.process.graph_traversal import GraphTraversalSource
        from gremlin_python.process.traversal import TraversalStrategies, GremlinLang
        from gremlin_python.driver.remote_connection import RemoteStrategy

        tx_connection = TransactionRemoteConnection(self)
        strategies = TraversalStrategies()
        strategies.add_strategies([RemoteStrategy(tx_connection)])
        return GraphTraversalSource(None, strategies, GremlinLang())

    def submit(self, gremlin, parameters=None, request_options=None):
        """Submits a gremlin-lang string within this transaction.

        The transactionId is automatically attached. Has the same signature
        as Client.submit() so both can be used interchangeably.
        """
        if not self._is_open:
            raise Exception("Transaction is not open")
        opts = {'transactionId': self._transaction_id}
        if request_options:
            opts.update(request_options)
        return self._client.submit(gremlin, parameters=parameters, request_options=opts)

    def commit(self):
        """Commits the transaction."""
        self._close_transaction("g.tx().commit()")

    def rollback(self):
        """Rolls back the transaction."""
        self._close_transaction("g.tx().rollback()")

    def _close_transaction(self, script):
        if not self._is_open:
            raise Exception("Transaction is not open")
        self._client.submit(script, request_options={'transactionId': self._transaction_id}).all().result()
        self._is_open = False
        self._failed = True  # Terminal state: transaction cannot be reused
        self._client.untrack_transaction(self)

    @property
    def is_open(self):
        return self._is_open

    @property
    def transaction_id(self):
        return self._transaction_id

    def close(self):
        if self._is_open:
            self.rollback()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._is_open:
            self.rollback()
        return False


class TransactionRemoteConnection(RemoteConnection):
    """A RemoteConnection that attaches transactionId to all requests submitted
    through the traversal API (g.tx().begin() -> gtx.addV()...).

    This bridges the traversal machinery (which calls RemoteConnection.submit)
    to the Transaction (which injects the transactionId).
    """

    def __init__(self, transaction):
        super().__init__(
            transaction._client._url,
            transaction._client._traversal_source)
        self._transaction = transaction

    def submit(self, gremlin_lang):
        if not self._transaction.is_open:
            raise Exception("Transaction is not open")

        gremlin_lang.add_g(self._traversal_source)

        from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
        request_options = DriverRemoteConnection.extract_request_options(gremlin_lang)
        request_options['transactionId'] = self._transaction.transaction_id

        result_set = self._transaction._client.submit(
            gremlin_lang.get_gremlin(), request_options=request_options)
        return RemoteTraversal(result_set)

    def is_closed(self):
        return not self._transaction.is_open
