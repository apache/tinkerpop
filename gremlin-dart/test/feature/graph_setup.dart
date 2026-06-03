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

import 'package:gremlin_dart/gremlin_dart.dart';

import 'cucumber_world.dart';

const graphTraversalSources = <String, String>{
  'modern': 'gmodern',
  'classic': 'gclassic',
  'crew': 'gcrew',
  'grateful': 'ggrateful',
  'sink': 'gsink',
  'empty': 'ggraph',
};

class GraphSetup {
  final String serverUrl;

  const GraphSetup(this.serverUrl);

  Future<Map<String, DataGraph>> loadAllDataGraphs() async {
    final graphs = <String, DataGraph>{};
    for (final entry in graphTraversalSources.entries) {
      final graph = await loadDataGraph(entry.key);
      graphs[entry.key] = graph;
    }
    return graphs;
  }

  Future<DataGraph> loadDataGraph(String name) async {
    final traversalSource = graphTraversalSources[name]!;
    final remote = DriverRemoteConnection(
      serverUrl,
      ConnectionOptions(traversalSource: traversalSource),
    );
    final graph = DataGraph(name: name, connection: remote);
    if (name == 'empty') return graph;

    graph.vertices = await _vertices(traversalSource);
    graph.edges = await _edges(traversalSource);
    graph.vertexProperties = await _vertexProperties(traversalSource);
    return graph;
  }

  Future<void> cleanEmptyGraph() async {
    await submit('g.V().drop()', 'ggraph');
  }

  Future<List<dynamic>> submit(
    String traversal,
    String traversalSource, [
    Map<String, dynamic>? params,
    Map<String, dynamic>? sideEffects,
  ]) async {
    // Substitute parameters directly into the gremlin string (same approach
    // as Python/Go drivers which inline values via DSL arguments).
    // Also strip .iterate() which is a client-side terminal — the server iterates for us.
    var resolved = GremlinLang.substituteParameters(traversal, params);
    resolved = resolved.replaceAll(RegExp(r'\.iterate\(\)\s*$'), '').trim();

    // Inject side effects as withSideEffect() calls after the leading "g"
    if (sideEffects != null && sideEffects.isNotEmpty) {
      final seStr = sideEffects.entries
          .map((e) => '.withSideEffect("${e.key}", ${GremlinLang.valueToGremlinLiteral(e.value)})')
          .join();
      // Insert after the leading "g" (before the first step)
      if (resolved.startsWith('g.') || resolved == 'g') {
        resolved = 'g$seStr${resolved.substring(1)}';
      }
    }
    final connection = Connection(
      serverUrl,
      ConnectionOptions(traversalSource: traversalSource),
    );
    try {
      final request = RequestMessage.build(resolved)
          .addG(traversalSource)
          .addBulkResults(true)
          .create();
      return (await connection.submit(request)).items;
    } finally {
      await connection.close();
    }
  }

  Future<void> refreshEmptyGraph(DataGraph graph) async {
    graph.vertices = await _vertices('ggraph');
    graph.edges = await _edges('ggraph');
    graph.vertexProperties = await _vertexProperties('ggraph');
  }

  Future<Map<String, Vertex>> _vertices(String traversalSource) async {
    final result = await submit(
      'g.V().group().by("name").by(__.tail())',
      traversalSource,
    );
    if (result.isEmpty || result.first is! Map) return <String, Vertex>{};
    return (result.first as Map).map((key, value) {
      return MapEntry(key.toString(), value as Vertex);
    });
  }

  Future<Map<String, Edge>> _edges(String traversalSource) async {
    final result = await submit(
      'g.E().group().by(__.project("o","l","i").by(__.outV().values("name")).by(__.label()).by(__.inV().values("name"))).by(__.tail())',
      traversalSource,
    );
    if (result.isEmpty || result.first is! Map) return <String, Edge>{};
    final edges = <String, Edge>{};
    (result.first as Map).forEach((key, value) {
      if (key is Map) {
        edges['${key['o']}-${key['l']}->${key['i']}'] = value as Edge;
      }
    });
    return edges;
  }

  Future<Map<String, VertexProperty>> _vertexProperties(
    String traversalSource,
  ) async {
    final result = await submit(
      'g.V().properties().group().by(__.project("n","k","v").by(__.element().values("name")).by(__.key()).by(__.value())).by(__.tail())',
      traversalSource,
    );
    if (result.isEmpty || result.first is! Map) {
      return <String, VertexProperty>{};
    }
    final vertexProperties = <String, VertexProperty>{};
    (result.first as Map).forEach((key, value) {
      if (key is Map) {
        vertexProperties[_vertexPropertyKey(key)] = value as VertexProperty;
      }
    });
    return vertexProperties;
  }

  String _vertexPropertyKey(Map<dynamic, dynamic> key) {
    final propertyKey = key['k'];
    var value = key['v'];
    if (propertyKey == 'weight') {
      value = 'd[$value].d';
    } else if (propertyKey == 'age' ||
        propertyKey == 'since' ||
        propertyKey == 'skill') {
      value = 'd[$value].i';
    }
    return '${key['n']}-$propertyKey->$value';
  }
}
