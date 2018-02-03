"""
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
"""
from concurrent.futures import Future

from gremlin_python.driver import client, serializer
from gremlin_python.driver.remote_connection import (
    RemoteConnection, RemoteTraversal, RemoteTraversalSideEffects)

__author__ = 'David M. Brown (davebshow@gmail.com)'


class DriverRemoteConnection(RemoteConnection):

    def __init__(self, url, traversal_source, protocol_factory=None,
                 transport_factory=None, pool_size=None, max_workers=None,
                 username="", password="", message_serializer=None,
                 graphson_reader=None, graphson_writer=None):
        if message_serializer is None:
            message_serializer = serializer.GraphSONMessageSerializer(
                reader=graphson_reader,
                writer=graphson_writer)
        self._client = client.Client(url, traversal_source,
                                     protocol_factory=protocol_factory,
                                     transport_factory=transport_factory,
                                     pool_size=pool_size,
                                     max_workers=max_workers,
                                     message_serializer=message_serializer,
                                     username=username,
                                     password=password)
        self._url = self._client._url
        self._traversal_source = self._client._traversal_source

    def close(self):
        self._client.close()

    def submit(self, bytecode):
        result_set = self._client.submit(bytecode)
        results = result_set.all().result()
        side_effects = RemoteTraversalSideEffects(result_set.request_id,
                                                  self._client)
        return RemoteTraversal(iter(results), side_effects)

    def submitAsync(self, bytecode):
        future = Future()
        future_result_set = self._client.submitAsync(bytecode)

        def cb(f):
            try:
                result_set = f.result()
                results = result_set.all().result()
                side_effects = RemoteTraversalSideEffects(result_set.request_id,
                                                          self._client)
                future.set_result(RemoteTraversal(iter(results), side_effects))
            except Exception as e:
                future.set_exception(e)

        future_result_set.add_done_callback(cb)
        return future
