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

from gremlin_python.process.traversal import TraversalStrategy


#########################
# DECORATION STRATEGIES #
#########################

class ConnectiveStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self)


class ElementIdStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self)


class HaltedTraverserStrategy(TraversalStrategy):
    def __init__(self, haltedTraverserFactory="detached"):
        TraversalStrategy.__init__(self, configuration={"haltedTraverserFactory": haltedTraverserFactory})


class PartitionStrategy(TraversalStrategy):
    def __init__(self, partitionKey, writePartition=None, readPartitions=None, includeMetaProperties=False):
        TraversalStrategy.__init__(self, configuration={"partitionKey": partitionKey,
                                                        "includeMetaProperties": includeMetaProperties})
        if writePartition is not None:
            self.configuration["writePartition"] = writePartition
        if writePartition is not None:
            self.configuration["readPartitions"] = readPartitions


class SubgraphStrategy(TraversalStrategy):
    def __init__(self, vertices=None, edges=None, vertexProperties=None):
        TraversalStrategy.__init__(self)
        if vertices is not None:
            self.configuration["vertices"] = vertices
        if edges is not None:
            self.configuration["edges"] = edges
        if vertexProperties is not None:
            self.configuration["vertexProperties"] = vertexProperties


###########################
# FINALIZATION STRATEGIES #
###########################

class MatchAlgorithmStrategy(TraversalStrategy):
    def __init__(self, matchAlgorithm="count"):
        TraversalStrategy.__init__(self, configuration={"matchAlgorithm": matchAlgorithm})


###########################
# OPTIMIZATION STRATEGIES #
###########################

class AdjacentToIncidentStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self)


class FilterRankingStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self)


class IdentityRemoveStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self)


class IncidentToAdjacentStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self)


class InlineFilterStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self)


class LazyBarrierStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self)


class MatchPredicateStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self)


class OrderLimitStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self)


class PathProcessorStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self)


class PathRetractionStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self)


class RangeByIsCountStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self)


class RepeatUnrollStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self)


###########################
# VERIFICATION STRATEGIES #
###########################

class LambdaRestrictionStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self)


class ReadOnlyStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self)
