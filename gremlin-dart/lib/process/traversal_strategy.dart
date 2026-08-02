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

// ---------------------------------------------------------------------------
// Common strategies
// ---------------------------------------------------------------------------

class OptionsStrategy extends TraversalStrategy {
  OptionsStrategy(Map<String, dynamic> options)
      : super(strategyName: 'OptionsStrategy', configuration: options);
}

class PartitionStrategy extends TraversalStrategy {
  PartitionStrategy({
    String? partitionKey,
    String? writePartition,
    List<String>? readPartitions,
    bool? includeMetaProperties,
  }) : super(
            strategyName: 'PartitionStrategy',
            configuration: {
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
  }) : super(
            strategyName: 'SubgraphStrategy',
            configuration: {
              if (vertices != null) 'vertices': vertices,
              if (edges != null) 'edges': edges,
              if (vertexProperties != null)
                'vertexProperties': vertexProperties,
              if (checkAdjacentVertices != null)
                'checkAdjacentVertices': checkAdjacentVertices,
            });
}

class SeedStrategy extends TraversalStrategy {
  SeedStrategy({required int seed})
      : super(
            strategyName: 'SeedStrategy',
            configuration: {'seed': seed});
}

class ReadOnlyStrategy extends TraversalStrategy {
  ReadOnlyStrategy() : super(strategyName: 'ReadOnlyStrategy');
}

class VertexProgramStrategy extends TraversalStrategy {
  VertexProgramStrategy(Map<String, dynamic> options)
      : super(strategyName: 'VertexProgramStrategy', configuration: options);
}
