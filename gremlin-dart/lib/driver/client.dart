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

import 'connection.dart';
import 'request_message.dart';
import 'result_set.dart';

class RequestOptions {
  final Map<String, dynamic>? bindings;
  final String? language;
  final int? evaluationTimeout;
  final bool? bulkResults;
  final String? materializeProperties;

  const RequestOptions({
    this.bindings,
    this.language,
    this.evaluationTimeout,
    this.bulkResults,
    this.materializeProperties,
  });
}

class Client {
  final Connection _connection;
  final ConnectionOptions options;

  Client(String url, [ConnectionOptions? options])
      : options = options ?? const ConnectionOptions(),
        _connection = Connection(url, options);

  bool get isOpen => _connection.isOpen;

  Future<void> open() => _connection.open();

  Future<ResultSet<dynamic>> submit(
    String message, {
    Map<String, dynamic>? bindings,
    RequestOptions? requestOptions,
  }) {
    return _connection.submit(
        _buildRequest(message, bindings: bindings, requestOptions: requestOptions));
  }

  Stream<dynamic> stream(
    String message, {
    Map<String, dynamic>? bindings,
    RequestOptions? requestOptions,
  }) {
    return _connection.stream(
        _buildRequest(message, bindings: bindings, requestOptions: requestOptions));
  }

  RequestMessage _buildRequest(
    String message, {
    Map<String, dynamic>? bindings,
    RequestOptions? requestOptions,
  }) {
    final builder = RequestMessage.build(message)
        .addG(options.traversalSource);

    if (requestOptions?.language != null) {
      builder.addLanguage(requestOptions!.language!);
    }
    if (requestOptions?.bindings != null) {
      builder.addBindings(requestOptions!.bindings!);
    }
    if (bindings != null) {
      builder.addBindings(bindings);
    }
    if (requestOptions?.materializeProperties != null) {
      builder.addMaterializeProperties(requestOptions!.materializeProperties!);
    }
    if (requestOptions?.evaluationTimeout != null) {
      builder.addTimeoutMillis(requestOptions!.evaluationTimeout!);
    }
    if (requestOptions?.bulkResults != null) {
      builder.addBulkResults(requestOptions!.bulkResults!);
    }

    return builder.create();
  }

  Future<void> close() => _connection.close();
}
