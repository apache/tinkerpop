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

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'

from .traversal import TraversalStrategy

base_namespace = 'org.apache.tinkerpop.gremlin.process.traversal.strategy.'
decoration_namespace = base_namespace + 'decoration.'
finalization_namespace = base_namespace + 'finalization.'
optimization_namespace = base_namespace + 'optimization.'
verification_namespace = base_namespace + 'verification.'
computer_decoration_namespace = 'org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.'

#########################
# DECORATION STRATEGIES #
#########################


class ConnectiveStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=decoration_namespace + 'ConnectiveStrategy')


class ElementIdStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=decoration_namespace + 'ElementIdStrategy')
        self.configuration = {}


# EventStrategy doesn't make sense outside JVM traversal machine

class HaltedTraverserStrategy(TraversalStrategy):
    def __init__(self, halted_traverser_factory=None):
        TraversalStrategy.__init__(self, fqcn=decoration_namespace + 'HaltedTraverserStrategy')
        if halted_traverser_factory is not None:
            self.configuration["haltedTraverserFactory"] = halted_traverser_factory


class OptionsStrategy(TraversalStrategy):
    def __init__(self, **options):
        TraversalStrategy.__init__(self, configuration=options, fqcn=decoration_namespace + 'OptionsStrategy')


class PartitionStrategy(TraversalStrategy):
    def __init__(self, partition_key=None, write_partition=None, read_partitions=None, include_meta_properties=None):
        TraversalStrategy.__init__(self, fqcn=decoration_namespace + 'PartitionStrategy')
        if partition_key is not None:
            self.configuration["partitionKey"] = partition_key
        if write_partition is not None:
            self.configuration["writePartition"] = write_partition
        if read_partitions is not None:
            self.configuration["readPartitions"] = read_partitions
        if include_meta_properties is not None:
            self.configuration["includeMetaProperties"] = include_meta_properties


class SeedStrategy(TraversalStrategy):
    def __init__(self, seed):
        TraversalStrategy.__init__(self, fqcn="org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy")
        self.configuration["seed"] = seed


class SubgraphStrategy(TraversalStrategy):

    def __init__(self, vertices=None, edges=None, vertex_properties=None, check_adjacent_vertices=None):
        TraversalStrategy.__init__(self, fqcn=decoration_namespace + 'SubgraphStrategy')
        if vertices is not None:
            self.configuration["vertices"] = vertices
        if edges is not None:
            self.configuration["edges"] = edges
        if vertex_properties is not None:
            self.configuration["vertexProperties"] = vertex_properties
        if check_adjacent_vertices is not None:
            self.configuration["checkAdjacentVertices"] = check_adjacent_vertices


class VertexProgramStrategy(TraversalStrategy):
    def __init__(self, graph_computer=None, workers=None, persist=None, result=None, vertices=None, edges=None,
                 configuration=None):
        TraversalStrategy.__init__(self, fqcn=computer_decoration_namespace + 'VertexProgramStrategy')
        if graph_computer is not None:
            self.configuration["graphComputer"] = graph_computer
        if workers is not None:
            self.configuration["workers"] = workers
        if persist is not None:
            self.configuration["persist"] = persist
        if result is not None:
            self.configuration["result"] = result
        if vertices is not None:
            self.configuration["vertices"] = vertices
        if edges is not None:
            self.configuration["edges"] = edges
        if configuration is not None:
            self.configuration.update(configuration)

class ReferenceElementStrategy(TraversalStrategy):
    def __init__(self, options=None):
        TraversalStrategy.__init__(self, configuration=options, fqcn=decoration_namespace + 'ReferenceElementStrategy')


class ComputerFinalizationStrategy(TraversalStrategy):
    def __init__(self, options=None):
        TraversalStrategy.__init__(self, configuration=options, fqcn=decoration_namespace + 'ComputerFinalizationStrategy')


class ProfileStrategy(TraversalStrategy):
    def __init__(self, options=None):
        TraversalStrategy.__init__(self, configuration=options, fqcn=decoration_namespace + 'ProfileStrategy')

###########################
# FINALIZATION STRATEGIES #
###########################

class MatchAlgorithmStrategy(TraversalStrategy):
    def __init__(self, match_algorithm=None):
        TraversalStrategy.__init__(self, fqcn=finalization_namespace + 'MatchAlgorithmStrategy')
        if match_algorithm is not None:
            self.configuration["matchAlgorithm"] = match_algorithm


###########################
# OPTIMIZATION STRATEGIES #
###########################

class AdjacentToIncidentStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'AdjacentToIncidentStrategy')


class ByModulatorOptimizationStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'ByModulatorOptimizationStrategy')


class CountStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'CountStrategy')


class FilterRankingStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'FilterRankingStrategy')


class IdentityRemovalStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'IdentityRemovalStrategy')


class IncidentToAdjacentStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'IncidentToAdjacentStrategy')


class InlineFilterStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'InlineFilterStrategy')


class LazyBarrierStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'LazyBarrierStrategy')


class MatchPredicateStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'MatchPredicateStrategy')


class OrderLimitStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'OrderLimitStrategy')


class PathProcessorStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'PathProcessorStrategy')


class PathRetractionStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'PathRetractionStrategy')


class ProductiveByStrategy(TraversalStrategy):
    def __init__(self, productiveKeys=None):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'ProductiveByStrategy')


class CountStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'CountStrategy')


class RepeatUnrollStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'RepeatUnrollStrategy')


class GraphFilterStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'GraphFilterStrategy')



class EarlyLimitStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=optimization_namespace + 'EarlyLimitStrategy')


class MessagePassingReductionStrategy(TraversalStrategy):
    def __init__(self, options=None):
        TraversalStrategy.__init__(self, configuration=options, fqcn=optimization_namespace + 'MessagePassingReductionStrategy')

###########################
# VERIFICATION STRATEGIES #
###########################

class ComputerVerificationStrategy(TraversalStrategy):
    def __init__(self, options=None):
        TraversalStrategy.__init__(self, configuration=options, fqcn=verification_namespace + 'ComputerVerificationStrategy')


class LambdaRestrictionStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=verification_namespace + 'LambdaRestrictionStrategy')

class ReadOnlyStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=verification_namespace + 'ReadOnlyStrategy')


class EdgeLabelVerificationStrategy(TraversalStrategy):
    def __init__(self, log_warning=False, throw_exception=False):
        TraversalStrategy.__init__(self, fqcn=verification_namespace + 'EdgeLabelVerificationStrategy')
        self.configuration["logWarning"] = log_warning
        self.configuration["throwException"] = throw_exception


class ReservedKeysVerificationStrategy(TraversalStrategy):
    def __init__(self, log_warning=False, throw_exception=False, keys=["id", "label"]):
        TraversalStrategy.__init__(self, fqcn=verification_namespace + 'ReservedKeysVerificationStrategy')
        self.configuration["logWarning"] = log_warning
        self.configuration["throwException"] = throw_exception
        self.configuration["keys"] = keys


class VertexProgramRestrictionStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=verification_namespace + 'VertexProgramRestrictionStrategy')


class StandardVerificationStrategy(TraversalStrategy):
    def __init__(self):
        TraversalStrategy.__init__(self, fqcn=verification_namespace + 'StandardVerificationStrategy')
