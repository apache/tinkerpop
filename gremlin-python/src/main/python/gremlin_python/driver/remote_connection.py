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
import abc

from gremlin_python.process import traversal

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com), Lyndon Bauto (lyndonb@bitquilltech.com)'


class RemoteConnection(object, metaclass=abc.ABCMeta):
    def __init__(self, url, traversal_source):
        self._url = url
        self._traversal_source = traversal_source

    @property
    def url(self):
        return self._url

    @property
    def traversal_source(self):
        return self._traversal_source

    @abc.abstractmethod
    def submit(self, bytecode):
        pass

    def is_closed(self):
        raise Exception('is_closed() must be implemented')

    def is_session_bound(self):
        return False

    def create_session(self):
        raise Exception('createSession() must be implemented');

    def commit(self):
        raise Exception('commit() must be implemented')

    def rollback(self):
        raise Exception('rollback() must be implemented')

    def __repr__(self):
        return "remoteconnection[" + self._url + "," + self._traversal_source + "]"


class RemoteTraversal(traversal.Traversal):
    def __init__(self, traversers):
        super(RemoteTraversal, self).__init__(None, None, None)
        self.traversers = traversers


class RemoteStrategy(traversal.TraversalStrategy):
    def __init__(self, remote_connection):
        # Gave this a fqcn that has a local "py:" prefix since this strategy isn't sent as bytecode to the server.
        # this is a sort of local-only strategy that actually executes client side. not sure if this prefix is the
        # right way to name this or not, but it should have a name to identify it.
        traversal.TraversalStrategy.__init__(self, fqcn="py:RemoteStrategy")
        self.remote_connection = remote_connection

    def apply(self, traversal):
        if traversal.traversers is None:
            remote_traversal = self.remote_connection.submit(traversal.bytecode)
            traversal.remote_results = remote_traversal
            traversal.traversers = remote_traversal.traversers

    def apply_async(self, traversal):
        if traversal.traversers is None:
            traversal.remote_results = self.remote_connection.submit_async(traversal.bytecode)
