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
import warnings
import queue
from concurrent.futures import ThreadPoolExecutor

from gremlin_python.driver import connection, request, serializer

log = logging.getLogger("gremlinpython")

# This is until concurrent.futures backport 3.1.0 release
try:
    from multiprocessing import cpu_count
except ImportError:
    # some platforms don't have multiprocessing
    def cpu_count():
        return None

__author__ = 'David M. Brown (davebshow@gmail.com), Lyndon Bauto (lyndonb@bitquilltech.com)'


class Client:

    def __init__(self, url, traversal_source, max_connections=None, max_workers=None,
                 response_serializer=None, interceptors=None, auth=None,
                 enable_user_agent_on_connect=True,
                 bulk_results=False, pdt_registry=None, default_batch_size=None,
                 **transport_kwargs):
        log.info("Creating Client with url '%s'", url)

        # pool_size is a deprecated alias for max_connections (deprecated as of 4.0.0), retained for one release.
        if 'pool_size' in transport_kwargs:
            warnings.warn(
                "As of release 4.0.0, the 'pool_size' option is deprecated and will be "
                "removed in a future release. Use 'max_connections' instead.",
                DeprecationWarning)
            pool_size = transport_kwargs.pop('pool_size')
            if max_connections is None:
                max_connections = pool_size

        self._closed = False
        self._url = url
        # A raw list is safe here because Python's GIL ensures list.append and
        # list.remove are atomic at the bytecode level.
        self._tracked_transactions = []
        self._enable_user_agent_on_connect = enable_user_agent_on_connect
        self._bulk_results = bulk_results
        self._traversal_source = traversal_source
        if default_batch_size is None:
            default_batch_size = 64
        self._default_batch_size = default_batch_size
        if response_serializer is None:
            response_serializer = serializer.GraphBinarySerializersV4()
        if pdt_registry is not None:
            response_serializer.configure_pdt_registry(pdt_registry)

        self._auth = auth
        self._response_serializer = response_serializer
        self._interceptors = interceptors

        self._transport_kwargs = transport_kwargs

        if max_connections is None:
            max_connections = 128
        self._max_connections = max_connections
        # This is until concurrent.futures backport 3.1.0 release
        if max_workers is None:
            # If your application is overlapping Gremlin I/O on multiple threads
            # consider passing kwarg max_workers = (cpu_count() or 1) * 5
            max_workers = max_connections
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        # Threadsafe queue
        self._pool = queue.Queue()
        self._fill_pool()

    @property
    def available_pool_size(self):
        return self._pool.qsize()
    
    def response_serializer(self):
        return self._response_serializer

    @property
    def executor(self):
        return self._executor

    @property
    def traversal_source(self):
        return self._traversal_source

    def _fill_pool(self):
        for i in range(self._max_connections):
            conn = self._get_connection()
            self._pool.put_nowait(conn)

    def is_closed(self):
        return self._closed

    def close(self):
        # prevent the Client from being closed more than once. it raises errors if new jobby jobs
        # get submitted to the executor when it is shutdown
        if self._closed:
            return

        # Best-effort rollback of any open transactions before closing connections
        txs = list(self._tracked_transactions)
        for tx in txs:
            try:
                tx.close()
            except Exception:
                pass
        self._tracked_transactions.clear()

        log.info("Closing Client with url '%s'", self._url)
        while not self._pool.empty():
            conn = self._pool.get(True)
            conn.close()
        self._executor.shutdown()
        self._closed = True

    def track_transaction(self, tx):
        self._tracked_transactions.append(tx)

    def untrack_transaction(self, tx):
        try:
            self._tracked_transactions.remove(tx)
        except ValueError:
            pass

    def transact(self):
        """Creates a new Transaction for executing operations within an explicit transaction.

        Transactions are short-lived and single-use. After commit or rollback,
        create a new Transaction for the next unit of work. The traversal source
        (g alias) is inherited from this Client.
        """
        from gremlin_python.driver.transaction import Transaction
        return Transaction(self)

    def _get_connection(self):
        return connection.Connection(
            self._url, self._traversal_source,
            self._executor, self._pool,
            response_serializer=self._response_serializer,
            auth=self._auth, interceptors=self._interceptors,
            enable_user_agent_on_connect=self._enable_user_agent_on_connect,
            bulk_results=self._bulk_results,
            max_connections=self._max_connections,
            **self._transport_kwargs)

    def submit(self, message, bindings=None, request_options=None):
        return self.submit_async(message, bindings=bindings, request_options=request_options).result()

    def submitAsync(self, message, bindings=None, request_options=None):
        warnings.warn(
            "gremlin_python.driver.client.Client.submitAsync will be replaced by "
            "gremlin_python.driver.client.Client.submit_async.",
            DeprecationWarning)
        return self.submit_async(message, bindings, request_options)

    def submit_async(self, message, bindings=None, request_options=None):
        if self.is_closed():
            raise Exception("Client is closed")

        log.debug("message '%s'", str(message))
        fields = {'g': self._traversal_source}

        # TODO: bindings is now part of request_options, evaluate the need to keep it separate in python.
        #  Note this bindings parameter only applies to string script submissions
        if isinstance(message, str) and bindings:
            from gremlin_python.process.traversal import GremlinLang
            if isinstance(bindings, dict):
                fields['bindings'] = GremlinLang.convert_parameters_to_string(bindings)
            else:
                fields['bindings'] = bindings

        if isinstance(message, str):
            log.debug("fields='%s', gremlin='%s'", str(fields), str(message))
            message = request.RequestMessage(fields=fields, gremlin=message)
        else:
            # A caller-supplied RequestMessage must not be mutated in place:
            # resubmitting the same message (e.g. on retry) would otherwise
            # accumulate request_options/batchSize from prior submits. Clone the
            # fields dict so this submit's mutations stay local, matching the
            # no-mutate contract of the .NET/JS drivers. Freshly built messages
            # (the string path above) already own a private fields dict.
            message = message._replace(fields=dict(message.fields))

        conn = self._pool.get(True)
        if request_options:
            message.fields.update({token: request_options[token] for token in request.Tokens
                                   if token in request_options and token != 'bindings'})
            if 'bindings' in request_options:
                bindings_val = request_options['bindings']
                if isinstance(bindings_val, dict):
                    from gremlin_python.process.traversal import GremlinLang
                    bindings_val = GremlinLang.convert_parameters_to_string(bindings_val)
                message.fields['bindings'] = bindings_val

        # Fill in the connection-level default batch size when the caller did
        # not set a per-request batchSize.
        if self._default_batch_size is not None and 'batchSize' not in message.fields:
            message.fields['batchSize'] = self._default_batch_size

        return conn.write(message)
