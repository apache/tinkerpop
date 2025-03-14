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

__author__ = 'Stephen Mallette (http://stephen.genoprime.com)'

from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import GraphTraversalSource
from gremlin_python.process.traversal import TraversalStrategies
from .. import statics

import warnings


class AnonymousTraversalSource(object):

    def __init__(self, traversal_source_class=GraphTraversalSource):
        self.traversal_source_class = traversal_source_class

    @classmethod
    def traversal(cls, traversal_source_class=GraphTraversalSource):
        return AnonymousTraversalSource(traversal_source_class)

    def with_(self, remote_connection):
        return self.traversal_source_class(Graph(), TraversalStrategies(), None, remote_connection)

    def withRemote(self, remote_connection):
        warnings.warn(
            "gremlin_python.process.AnonymousTraversalSource.withRemote will be replaced by "
            "gremlin_python.process.AnonymousTraversalSource.with_.",
            DeprecationWarning)
        return self.with_(remote_connection)

    def with_remote(self, remote_connection):
        warnings.warn(
            "gremlin_python.process.AnonymousTraversalSource.with_remote will be replaced by "
            "gremlin_python.process.AnonymousTraversalSource.with_.",
            DeprecationWarning)
        return self.with_(remote_connection)


def traversal(traversal_source_class=GraphTraversalSource):
    return AnonymousTraversalSource.traversal(traversal_source_class)


statics.add_static('traversal', traversal)
