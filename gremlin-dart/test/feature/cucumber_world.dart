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

class CucumberWorld {
  final String serverUrl;
  GraphTraversalSource? g;
  String graphName = '';
  String? pendingTraversal;
  Map<String, dynamic> params = <String, dynamic>{};
  List<dynamic> result = <dynamic>[];
  bool resultIsNext = false;
  String? errorMessage;
  bool ignore = false;
  Map<String, DataGraph> graphDataMap;
  Set<String> tags = <String>{};

  CucumberWorld(this.serverUrl, [Map<String, DataGraph>? graphDataMap])
      : graphDataMap = graphDataMap ?? <String, DataGraph>{};

  Map<String, dynamic> sideEffects = <String, dynamic>{};

  void resetScenario(Set<String> scenarioTags) {
    tags = scenarioTags;
    graphName = '';
    pendingTraversal = null;
    params = <String, dynamic>{};
    sideEffects = <String, dynamic>{};
    result = <dynamic>[];
    resultIsNext = false;
    errorMessage = null;
    ignore = false;
    g = null;
  }
}

class DataGraph {
  final String name;
  final DriverRemoteConnection connection;
  Map<String, Vertex> vertices;
  Map<String, Edge> edges;
  Map<String, VertexProperty> vertexProperties;

  DataGraph({
    required this.name,
    required this.connection,
    Map<String, Vertex>? vertices,
    Map<String, Edge>? edges,
    Map<String, VertexProperty>? vertexProperties,
  })  : vertices = vertices ?? <String, Vertex>{},
        edges = edges ?? <String, Edge>{},
        vertexProperties = vertexProperties ?? <String, VertexProperty>{};
}
