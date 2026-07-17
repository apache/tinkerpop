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
from concurrent.futures import Future
import warnings

from gremlin_python.driver import client, serializer
from gremlin_python.driver.remote_connection import RemoteConnection, RemoteTraversal
from gremlin_python.driver.request import Tokens

log = logging.getLogger("gremlinpython")

__author__ = 'David M. Brown (davebshow@gmail.com), Lyndon Bauto (lyndonb@bitquilltech.com)'


class DriverRemoteConnection(RemoteConnection):

    def __init__(self, url, traversal_source="g",
                 max_connections=128, max_workers=None,
                 response_serializer=None, interceptors=None, auth=None,
                 enable_user_agent_on_connect=True,
                 bulk_results=False, pdt_registry=None, batch_size=None,
                 **transport_kwargs):
        log.info("Creating DriverRemoteConnection with url '%s'", str(url))
        self.__url = url
        self.__traversal_source = traversal_source
        self.__max_connections = max_connections
        self.__max_workers = max_workers
        self.__auth = auth
        self.__enable_user_agent_on_connect = enable_user_agent_on_connect
        self.__bulk_results = bulk_results
        self.__transport_kwargs = transport_kwargs
        self.pdt_registry = pdt_registry

        if response_serializer is None:
            response_serializer = serializer.GraphBinarySerializersV4()
        self._client = client.Client(url, traversal_source,
                                     max_connections=max_connections,
                                     max_workers=max_workers,
                                     response_serializer=response_serializer,
                                     interceptors=interceptors, auth=auth,
                                     enable_user_agent_on_connect=enable_user_agent_on_connect,
                                     bulk_results=bulk_results,
                                     pdt_registry=pdt_registry,
                                     batch_size=batch_size,
                                     **transport_kwargs)
        self._url = self._client._url
        self._traversal_source = self._client._traversal_source

    def close(self):
        log.info("closing DriverRemoteConnection with url '%s'", str(self._url))

        self._client.close()

    def submit(self, gremlin_lang):
        log.debug("submit with gremlin lang script '%s'", gremlin_lang.get_gremlin())
        gremlin_lang.add_g(self._traversal_source)
        result_set = self._client.submit(gremlin_lang.get_gremlin(),
                                         request_options=self.extract_request_options(gremlin_lang))
        return RemoteTraversal(result_set)

    def submitAsync(self, gremlin_lang):
        warnings.warn(
            "gremlin_python.driver.driver_remote_connection.DriverRemoteConnection.submitAsync will be replaced by "
            "gremlin_python.driver.driver_remote_connection.DriverRemoteConnection.submit_async.",
            DeprecationWarning)
        return self.submit_async(gremlin_lang)

    def submit_async(self, gremlin_lang):
        log.debug("submit_async with gremlin lang script '%s'", gremlin_lang.get_gremlin())
        future = Future()
        gremlin_lang.add_g(self._traversal_source)
        future_result_set = self._client.submit_async(gremlin_lang.get_gremlin(),
                                                      request_options=self.extract_request_options(gremlin_lang))

        def cb(f):
            try:
                result_set = f.result()
                future.set_result(RemoteTraversal(result_set))
            except Exception as e:
                future.set_exception(e)

        future_result_set.add_done_callback(cb)
        return future

    def is_closed(self):
        return self._client.is_closed()

    @staticmethod
    def extract_request_options(gremlin_lang):
        request_options = {}
        for os in gremlin_lang.options_strategies:
            request_options.update({token: os.configuration[token] for token in Tokens
                                    if token in os.configuration})
        # request the server to bulk results by default when using drc through request options
        if 'bulkResults' not in request_options:
            request_options['bulkResults'] = True

        parameters_string = gremlin_lang.get_parameters_as_string()
        if parameters_string != '[:]':
            request_options["parameters"] = parameters_string

        return request_options
