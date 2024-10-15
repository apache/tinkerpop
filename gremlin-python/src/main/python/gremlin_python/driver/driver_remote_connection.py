#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
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
from gremlin_python.driver.remote_connection import (
    RemoteConnection, RemoteTraversal)
from gremlin_python.process.strategies import OptionsStrategy
from gremlin_python.process.traversal import Bytecode
import uuid

log = logging.getLogger("gremlinpython")

__author__ = 'David M. Brown (davebshow@gmail.com), Lyndon Bauto (lyndonb@bitquilltech.com)'


class DriverRemoteConnection(RemoteConnection):

    def __init__(self, url, traversal_source="g", protocol_factory=None,
                 transport_factory=None, pool_size=None, max_workers=None,
                 username="", password="", kerberized_service='',
                 message_serializer=None, graphson_reader=None,
                 graphson_writer=None, headers=None, session=None,
                 enable_user_agent_on_connect=True, enable_compression=False, **transport_kwargs):
        log.info("Creating DriverRemoteConnection with url '%s'", str(url))
        self.__url = url
        self.__traversal_source = traversal_source
        self.__protocol_factory = protocol_factory
        self.__transport_factory = transport_factory
        self.__pool_size = pool_size
        self.__max_workers = max_workers
        self.__username = username
        self.__password = password
        self.__kerberized_service = kerberized_service
        self.__message_serializer = message_serializer
        self.__graphson_reader = graphson_reader
        self.__graphson_writer = graphson_writer
        self.__headers = headers
        self.__session = session
        self.__enable_user_agent_on_connect = enable_user_agent_on_connect
        self.__enable_compression = enable_compression
        self.__transport_kwargs = transport_kwargs

        # keeps a list of sessions that have been spawned from this DriverRemoteConnection
        # so that they can be closed if this parent session is closed.
        self.__spawned_sessions = []

        if message_serializer is None and graphson_reader is not None and graphson_writer is not None:
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
                                     password=password,
                                     kerberized_service=kerberized_service,
                                     headers=headers,
                                     session=session,
                                     enable_user_agent_on_connect=enable_user_agent_on_connect,
                                     enable_compression=enable_compression,
                                     **transport_kwargs)
        self._url = self._client._url
        self._traversal_source = self._client._traversal_source

    def close(self):
        # close this client and any DriverRemoteConnection instances spawned from this one
        # for a session
        if len(self.__spawned_sessions) > 0:
            log.info("closing spawned sessions from DriverRemoteConnection with url '%s'", str(self._url))
            for spawned_session in self.__spawned_sessions:
                spawned_session.close()
            self.__spawned_sessions.clear()

        if self.__session:
            log.info("closing DriverRemoteConnection with url '%s' with session '%s'",
                         str(self._url), str(self.__session))
        else:
            log.info("closing DriverRemoteConnection with url '%s'", str(self._url))

        self._client.close()

    def submit(self, bytecode):
        log.debug("submit with bytecode '%s'", str(bytecode))
        result_set = self._client.submit(bytecode, request_options=self._extract_request_options(bytecode))
        results = result_set.all().result()
        return RemoteTraversal(iter(results))

    def submitAsync(self, message, bindings=None, request_options=None):
        warnings.warn(
            "gremlin_python.driver.driver_remote_connection.DriverRemoteConnection.submitAsync will be replaced by "
            "gremlin_python.driver.driver_remote_connection.DriverRemoteConnection.submit_async.",
            DeprecationWarning)
        self.submit_async(message, bindings, request_options)

    def submit_async(self, bytecode):
        log.debug("submit_async with bytecode '%s'", str(bytecode))
        future = Future()
        future_result_set = self._client.submit_async(bytecode, request_options=self._extract_request_options(bytecode))

        def cb(f):
            try:
                result_set = f.result()
                results = result_set.all().result()
                future.set_result(RemoteTraversal(iter(results)))
            except Exception as e:
                future.set_exception(e)

        future_result_set.add_done_callback(cb)
        return future

    def is_closed(self):
        return self._client.is_closed()

    def is_session_bound(self):
        return self.__session is not None

    def create_session(self):
        log.info("Creating session based connection")
        if self.is_session_bound():
            raise Exception('Connection is already bound to a session - child sessions are not allowed')
        conn = DriverRemoteConnection(self.__url,
                                      traversal_source=self.__traversal_source,
                                      protocol_factory=self.__protocol_factory,
                                      transport_factory=self.__transport_factory,
                                      pool_size=self.__pool_size,
                                      max_workers=self.__max_workers,
                                      username=self.__username,
                                      password=self.__password,
                                      kerberized_service=self.__kerberized_service,
                                      message_serializer=self.__message_serializer,
                                      graphson_reader=self.__graphson_reader,
                                      graphson_writer=self.__graphson_writer,
                                      headers=self.__headers,
                                      session=uuid.uuid4(),
                                      enable_user_agent_on_connect=self.__enable_user_agent_on_connect,
                                      **self.__transport_kwargs)
        self.__spawned_sessions.append(conn)
        return conn

    def remove_session(self, session_based_connection):
        session_based_connection.close()
        self.__spawned_sessions.remove(session_based_connection)

    def commit(self):
        log.info("Submitting commit graph operation.")
        return self._client.submit(Bytecode.GraphOp.commit())

    def rollback(self):
        log.info("Submitting rollback graph operation.")
        return self._client.submit(Bytecode.GraphOp.rollback())

    @staticmethod
    def _extract_request_options(bytecode):
        options_strategy = next((x for x in bytecode.source_instructions
                                 if x[0] == "withStrategies" and type(x[1]) is OptionsStrategy), None)
        request_options = None
        if options_strategy:
            allowed_keys = ['evaluationTimeout', 'scriptEvaluationTimeout', 'batchSize', 'requestId', 'userAgent', 'materializeProperties']
            request_options = {allowed: options_strategy[1].configuration[allowed] for allowed in allowed_keys
                               if allowed in options_strategy[1].configuration}
        return request_options
