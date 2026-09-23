// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import '../driver/transaction.dart';
import 'gremlin_lang.dart';
import 'traversal.dart';

class TraversalStrategies {
  final List<TraversalStrategy> strategies;

  TraversalStrategies([TraversalStrategies? parent])
      : strategies = List.of(parent?.strategies ?? []);

  void addStrategy(TraversalStrategy strategy) => strategies.add(strategy);

  void removeStrategy(TraversalStrategy strategy) =>
      strategies.removeWhere((s) => s.strategyName == strategy.strategyName);

  Future<void> applyStrategies(Traversal traversal) async {
    for (final s in strategies) {
      await s.apply(traversal);
    }
  }
}

abstract class TraversalStrategy {
  final String strategyName;
  final Map<String, dynamic> configuration;

  TraversalStrategy({String? strategyName, Map<String, dynamic>? configuration})
      : strategyName = strategyName ?? '',
        configuration = configuration ?? {};

  Future<void> apply(Traversal traversal) async {}
}

/// Resolves a strategy instance, class literal, or explicit name to the
/// server-side strategy name without relying on [Type.toString()].
String strategyNameOf(dynamic strategy) {
  if (strategy is String) return strategy;
  if (strategy is TraversalStrategy) return strategy.strategyName;

  const names = <Type, String>{
    RemoteStrategy: 'RemoteStrategy',
    OptionsStrategy: 'OptionsStrategy',
    PartitionStrategy: 'PartitionStrategy',
    SubgraphStrategy: 'SubgraphStrategy',
    SeedStrategy: 'SeedStrategy',
    ReadOnlyStrategy: 'ReadOnlyStrategy',
    VertexProgramStrategy: 'VertexProgramStrategy',
    AdjacentToIncidentStrategy: 'AdjacentToIncidentStrategy',
    ByModulatorOptimizationStrategy: 'ByModulatorOptimizationStrategy',
    ComputerFinalizationStrategy: 'ComputerFinalizationStrategy',
    ComputerVerificationStrategy: 'ComputerVerificationStrategy',
    ConnectiveStrategy: 'ConnectiveStrategy',
    CountStrategy: 'CountStrategy',
    EarlyLimitStrategy: 'EarlyLimitStrategy',
    ElementIdStrategy: 'ElementIdStrategy',
    FilterRankingStrategy: 'FilterRankingStrategy',
    GraphFilterStrategy: 'GraphFilterStrategy',
    IdentityRemovalStrategy: 'IdentityRemovalStrategy',
    IncidentToAdjacentStrategy: 'IncidentToAdjacentStrategy',
    InlineFilterStrategy: 'InlineFilterStrategy',
    LambdaRestrictionStrategy: 'LambdaRestrictionStrategy',
    LazyBarrierStrategy: 'LazyBarrierStrategy',
    MatchPredicateStrategy: 'MatchPredicateStrategy',
    MessagePassingReductionStrategy: 'MessagePassingReductionStrategy',
    OrderLimitStrategy: 'OrderLimitStrategy',
    PathProcessorStrategy: 'PathProcessorStrategy',
    PathRetractionStrategy: 'PathRetractionStrategy',
    ProductiveByStrategy: 'ProductiveByStrategy',
    ProfileStrategy: 'ProfileStrategy',
    ReferenceElementStrategy: 'ReferenceElementStrategy',
    RepeatUnrollStrategy: 'RepeatUnrollStrategy',
    StandardVerificationStrategy: 'StandardVerificationStrategy',
    VertexProgramRestrictionStrategy: 'VertexProgramRestrictionStrategy',
    ReservedKeysVerificationStrategy: 'ReservedKeysVerificationStrategy',
    EdgeLabelVerificationStrategy: 'EdgeLabelVerificationStrategy',
    MatchAlgorithmStrategy: 'MatchAlgorithmStrategy',
    HaltedTraverserStrategy: 'HaltedTraverserStrategy',
  };
  final name = names[strategy];
  if (name == null) {
    throw ArgumentError.value(strategy, 'strategy', 'Unsupported strategy');
  }
  return name;
}

// ---------------------------------------------------------------------------
// RemoteStrategy — wires a RemoteConnection to a traversal
// ---------------------------------------------------------------------------

class RemoteStrategy extends TraversalStrategy {
  final RemoteConnectionBase connection;

  RemoteStrategy(this.connection) : super(strategyName: 'RemoteStrategy');

  @override
  Future<void> apply(Traversal traversal) async {
    if (traversal.resultsStream != null) return;
    final remote = await connection.submit(traversal.getGremlinLang());
    traversal.resultsStream = remote.resultsStream;
  }
}

// Forward declaration to avoid circular import — concrete class lives in
// driver/remote_connection.dart.
abstract class RemoteConnectionBase {
  Future<RemoteTraversal> submit(GremlinLang gremlinLang);
}

abstract class TransactionCapableRemoteConnectionBase
    implements RemoteConnectionBase {
  Transaction tx([String? traversalSource]);
}

// ---------------------------------------------------------------------------
// Common strategies
// ---------------------------------------------------------------------------

class OptionsStrategy extends TraversalStrategy {
  OptionsStrategy([Map<String, dynamic>? options])
      : super(strategyName: 'OptionsStrategy', configuration: options);
}

class PartitionStrategy extends TraversalStrategy {
  PartitionStrategy({
    String? partitionKey,
    String? writePartition,
    List<String>? readPartitions,
    bool? includeMetaProperties,
  }) : super(strategyName: 'PartitionStrategy', configuration: {
          if (partitionKey != null) 'partitionKey': partitionKey,
          if (writePartition != null) 'writePartition': writePartition,
          if (readPartitions != null) 'readPartitions': readPartitions,
          if (includeMetaProperties != null)
            'includeMetaProperties': includeMetaProperties,
        });
}

class SubgraphStrategy extends TraversalStrategy {
  SubgraphStrategy({
    dynamic vertices,
    dynamic edges,
    dynamic vertexProperties,
    bool? checkAdjacentVertices,
  }) : super(strategyName: 'SubgraphStrategy', configuration: {
          if (vertices != null) 'vertices': vertices,
          if (edges != null) 'edges': edges,
          if (vertexProperties != null) 'vertexProperties': vertexProperties,
          if (checkAdjacentVertices != null)
            'checkAdjacentVertices': checkAdjacentVertices,
        });
}

class SeedStrategy extends TraversalStrategy {
  SeedStrategy({required dynamic seed})
      : super(
            strategyName: 'SeedStrategy',
            configuration: {'seed': seed is GInt ? seed.value : seed});
}

class ReadOnlyStrategy extends TraversalStrategy {
  ReadOnlyStrategy() : super(strategyName: 'ReadOnlyStrategy');
}

class VertexProgramStrategy extends TraversalStrategy {
  VertexProgramStrategy([Map<String, dynamic>? options])
      : super(strategyName: 'VertexProgramStrategy', configuration: options);
}

// Strategies without configuration.
class AdjacentToIncidentStrategy extends TraversalStrategy {
  AdjacentToIncidentStrategy()
      : super(strategyName: 'AdjacentToIncidentStrategy');
}

class ByModulatorOptimizationStrategy extends TraversalStrategy {
  ByModulatorOptimizationStrategy()
      : super(strategyName: 'ByModulatorOptimizationStrategy');
}

class ComputerFinalizationStrategy extends TraversalStrategy {
  ComputerFinalizationStrategy()
      : super(strategyName: 'ComputerFinalizationStrategy');
}

class ComputerVerificationStrategy extends TraversalStrategy {
  ComputerVerificationStrategy()
      : super(strategyName: 'ComputerVerificationStrategy');
}

class ConnectiveStrategy extends TraversalStrategy {
  ConnectiveStrategy() : super(strategyName: 'ConnectiveStrategy');
}

class CountStrategy extends TraversalStrategy {
  CountStrategy() : super(strategyName: 'CountStrategy');
}

class EarlyLimitStrategy extends TraversalStrategy {
  EarlyLimitStrategy() : super(strategyName: 'EarlyLimitStrategy');
}

class ElementIdStrategy extends TraversalStrategy {
  ElementIdStrategy() : super(strategyName: 'ElementIdStrategy');
}

class FilterRankingStrategy extends TraversalStrategy {
  FilterRankingStrategy() : super(strategyName: 'FilterRankingStrategy');
}

class GraphFilterStrategy extends TraversalStrategy {
  GraphFilterStrategy() : super(strategyName: 'GraphFilterStrategy');
}

class IdentityRemovalStrategy extends TraversalStrategy {
  IdentityRemovalStrategy() : super(strategyName: 'IdentityRemovalStrategy');
}

class IncidentToAdjacentStrategy extends TraversalStrategy {
  IncidentToAdjacentStrategy()
      : super(strategyName: 'IncidentToAdjacentStrategy');
}

class InlineFilterStrategy extends TraversalStrategy {
  InlineFilterStrategy() : super(strategyName: 'InlineFilterStrategy');
}

class LambdaRestrictionStrategy extends TraversalStrategy {
  LambdaRestrictionStrategy()
      : super(strategyName: 'LambdaRestrictionStrategy');
}

class LazyBarrierStrategy extends TraversalStrategy {
  LazyBarrierStrategy() : super(strategyName: 'LazyBarrierStrategy');
}

class MatchPredicateStrategy extends TraversalStrategy {
  MatchPredicateStrategy() : super(strategyName: 'MatchPredicateStrategy');
}

class MessagePassingReductionStrategy extends TraversalStrategy {
  MessagePassingReductionStrategy()
      : super(strategyName: 'MessagePassingReductionStrategy');
}

class OrderLimitStrategy extends TraversalStrategy {
  OrderLimitStrategy() : super(strategyName: 'OrderLimitStrategy');
}

class PathProcessorStrategy extends TraversalStrategy {
  PathProcessorStrategy() : super(strategyName: 'PathProcessorStrategy');
}

class PathRetractionStrategy extends TraversalStrategy {
  PathRetractionStrategy() : super(strategyName: 'PathRetractionStrategy');
}

class ProductiveByStrategy extends TraversalStrategy {
  ProductiveByStrategy() : super(strategyName: 'ProductiveByStrategy');
}

class ProfileStrategy extends TraversalStrategy {
  ProfileStrategy() : super(strategyName: 'ProfileStrategy');
}

class ReferenceElementStrategy extends TraversalStrategy {
  ReferenceElementStrategy() : super(strategyName: 'ReferenceElementStrategy');
}

class RepeatUnrollStrategy extends TraversalStrategy {
  RepeatUnrollStrategy() : super(strategyName: 'RepeatUnrollStrategy');
}

class StandardVerificationStrategy extends TraversalStrategy {
  StandardVerificationStrategy()
      : super(strategyName: 'StandardVerificationStrategy');
}

class VertexProgramRestrictionStrategy extends TraversalStrategy {
  VertexProgramRestrictionStrategy()
      : super(strategyName: 'VertexProgramRestrictionStrategy');
}

// Strategies with configuration.
class ReservedKeysVerificationStrategy extends TraversalStrategy {
  ReservedKeysVerificationStrategy(
      {bool? throwException, bool? logWarning, dynamic keys})
      : super(strategyName: 'ReservedKeysVerificationStrategy', configuration: {
          if (throwException != null) 'throwException': throwException,
          if (logWarning != null) 'logWarning': logWarning,
          if (keys != null) 'keys': keys,
        });
}

class EdgeLabelVerificationStrategy extends TraversalStrategy {
  EdgeLabelVerificationStrategy({bool? throwException, bool? logWarning})
      : super(strategyName: 'EdgeLabelVerificationStrategy', configuration: {
          if (throwException != null) 'throwException': throwException,
          if (logWarning != null) 'logWarning': logWarning,
        });
}

class MatchAlgorithmStrategy extends TraversalStrategy {
  MatchAlgorithmStrategy({String? matchAlgorithm})
      : super(strategyName: 'MatchAlgorithmStrategy', configuration: {
          if (matchAlgorithm != null) 'matchAlgorithm': matchAlgorithm,
        });
}

class HaltedTraverserStrategy extends TraversalStrategy {
  HaltedTraverserStrategy({String? haltedTraverserFactory})
      : super(strategyName: 'HaltedTraverserStrategy', configuration: {
          if (haltedTraverserFactory != null)
            'haltedTraverserFactory': haltedTraverserFactory,
        });
}
