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

import '../process/gremlin_lang.dart';
import '../process/traversal.dart';
import 'client.dart';
import 'connection.dart';
import 'remote_connection.dart';

class DriverRemoteConnection extends RemoteConnection {
  final Client _client;

  DriverRemoteConnection(String url, [ConnectionOptions? options])
      : _client = Client(url, options),
        super(url);

  @override
  Future<void> open() => _client.open();

  @override
  bool get isOpen => _client.isOpen;

  @override
  Future<RemoteTraversal> submit(GremlinLang gremlinLang) {
    final (gremlin, requestOptions) = _buildRequestArgs(gremlinLang);
    final stream = _client.stream(gremlin, requestOptions: requestOptions);
    return Future.value(RemoteTraversal(stream));
  }

  (String, RequestOptions) _buildRequestArgs(GremlinLang gremlinLang) {
    gremlinLang.addG(_client.options.traversalSource);

    final strategies = gremlinLang.getOptionsStrategies();
    final allowed = {
      'evaluationTimeout',
      'batchSize',
      'userAgent',
      'materializeProperties',
      'bulkResults',
    };

    int? evalTimeout;
    bool? bulkResults;
    String? materializeProperties;

    for (final s in strategies) {
      for (final entry in s.configuration.entries) {
        if (!allowed.contains(entry.key)) continue;
        switch (entry.key) {
          case 'evaluationTimeout':
            evalTimeout = entry.value as int?;
          case 'bulkResults':
            bulkResults = entry.value as bool?;
          case 'materializeProperties':
            materializeProperties = entry.value as String?;
        }
      }
    }

    bulkResults ??= true;

    final requestOptions = RequestOptions(
      evaluationTimeout: evalTimeout,
      bulkResults: bulkResults,
      materializeProperties: materializeProperties,
      // bindings is a pre-formatted string here; client handles it below
    );

    return (gremlinLang.getGremlin(), requestOptions);
  }

  @override
  Future<void> commit() async {
    await _client.submit('g.tx().commit()');
  }

  @override
  Future<void> rollback() async {
    await _client.submit('g.tx().rollback()');
  }

  @override
  Future<void> close() => _client.close();
}
