'''
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
'''
import abc
import six

from gremlin_python.driver import request
from gremlin_python.process import traversal

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'


@six.add_metaclass(abc.ABCMeta)
class RemoteConnection(object):
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

    def __repr__(self):
        return "remoteconnection[" + self._url + "," + self._traversal_source + "]"


class RemoteTraversal(traversal.Traversal):
    def __init__(self, traversers, side_effects):
        super(RemoteTraversal, self).__init__(None, None, None)
        self.traversers = traversers
        self._side_effects = side_effects

    @property
    def side_effects(self):
        return self._side_effects

    @side_effects.setter
    def side_effects(self, val):
        self._side_effects = val


class RemoteTraversalSideEffects(traversal.TraversalSideEffects):
    def __init__(self, side_effect, client):
        self._side_effect = side_effect
        self._client = client
        self._keys = set()
        self._side_effects = {}
        self._closed = False

    def keys(self):
        if not self._closed:
            message = request.RequestMessage(
                'traversal', 'keys',
                {'sideEffect': self._side_effect,
                'aliases': {'g': self._client.traversal_source}})
            self._keys = set(self._client.submit(message).all().result())
        return self._keys

    def get(self, key):

        if not self._side_effects.get(key):
            if not self._closed:
                message = request.RequestMessage(
                    'traversal', 'gather',
                    {'sideEffect': self._side_effect, 'sideEffectKey': key,
                     'aliases': {'g': self._client.traversal_source}})
                results = self._aggregate_results(self._client.submit(message))
                self._side_effects[key] = results
                self._keys.add(key)
            else:
                return None
        return self._side_effects[key]

    def close(self):
        if not self._closed:
            message = request.RequestMessage(
                'traversal', 'close',
                {'sideEffect': self._side_effect,
                 'aliases': {'g': self._client._traversal_source}})
            results = self._client.submit(message).all().result()
        self._closed = True
        return results

    def _aggregate_results(self, result_set):
        aggregates = {'list': [], 'set': set(), 'map': {}, 'bulkset': {},
                      'none': None}
        results = None
        for msg in result_set:
            if results is None:
                aggregate_to = result_set.aggregate_to
                results = aggregates.get(aggregate_to, [])
            # on first message, get the right result data structure
            # if there is no update to a structure, then the item is the result
            if results is None:
                results = msg[0]
            # updating a map is different than a list or a set
            elif isinstance(results, dict):
                if aggregate_to == "map":
                    for item in msg:
                        results.update(item)
                else:
                    for item in msg:
                        results[item.object] = item.bulk
            elif isinstance(results, set):
                results.update(msg)
            # flat add list to result list
            else:
                results += msg
        if results is None:
            results = []
        return results


class RemoteStrategy(traversal.TraversalStrategy):
    def __init__(self, remote_connection):
        self.remote_connection = remote_connection

    def apply(self, traversal):
        if traversal.traversers is None:
            remote_traversal = self.remote_connection.submit(traversal.bytecode)
            traversal.remote_results = remote_traversal
            traversal.side_effects = remote_traversal.side_effects
            traversal.traversers = remote_traversal.traversers

    def apply_async(self, traversal):
        if traversal.traversers is None:
            traversal.remote_results = self.remote_connection.submitAsync(
                traversal.bytecode)
