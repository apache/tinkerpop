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

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'

from graph import Graph
from gremlin_python.process.traversal import TraversalStrategies
from gremlin_python.process.traversal import TraversalStrategy


class RemoteGraph(Graph):
    def __init__(self, remote_connection):
        TraversalStrategies.global_cache[self.__class__] = TraversalStrategies([RemoteStrategy()])
        self.remote_connection = remote_connection

    def __repr__(self):
        return "remotegraph[" + self.remote_connection.url + "]"


class RemoteStrategy(TraversalStrategy):
    def apply(self, traversal):
        if not (traversal.graph.__class__.__name__ == "RemoteGraph"):
            raise BaseException(
                "RemoteStrategy can only be used with a RemoteGraph: " + traversal.graph.__class__.__name__)
        if traversal.traversers is None:
            remote_response = traversal.graph.remote_connection.submit(
                'gremlin-groovy',  # script engine
                traversal.bytecode),  # script
            traversal.side_effects = remote_response.side_effects
            traversal.traversers = remote_response.traversers
        return
