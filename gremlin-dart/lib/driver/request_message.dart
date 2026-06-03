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

class RequestMessage {
  final String gremlin;
  final String language;
  final int? timeoutMs;
  final String? bindings;
  final String? g;
  final String? materializeProperties;
  final bool? bulkResults;
  final Map<String, dynamic> fields;

  static const _matTokens = 'tokens';
  static const _matAll = 'all';

  const RequestMessage._({
    required this.gremlin,
    required this.language,
    this.timeoutMs,
    this.bindings,
    this.g,
    this.materializeProperties,
    this.bulkResults,
    this.fields = const {},
  });

  static RequestMessageBuilder build(String gremlin) =>
      RequestMessageBuilder._(gremlin);

  Map<String, dynamic> toJson() {
    final m = <String, dynamic>{'gremlin': gremlin, 'language': language};
    if (bindings != null) m['bindings'] = bindings;
    if (g != null) m['g'] = g;
    if (materializeProperties != null) {
      m['materializeProperties'] = materializeProperties;
    }
    if (timeoutMs != null) m['timeoutMs'] = timeoutMs;
    if (bulkResults != null) m['bulkResults'] = bulkResults;
    m.addAll(fields);
    return m;
  }
}

class RequestMessageBuilder {
  final String _gremlin;
  String _language = 'gremlin-lang';
  int? _timeoutMs;
  final Map<String, dynamic> _bindings = {};
  String? _bindingsString;
  String? _g;
  String? _materializeProperties;
  bool? _bulkResults;
  final Map<String, dynamic> _fields = {};

  RequestMessageBuilder._(this._gremlin);

  RequestMessageBuilder addLanguage(String language) {
    _language = language;
    return this;
  }

  RequestMessageBuilder addBinding(String key, dynamic value) {
    if (_bindingsString != null) {
      throw StateError('Cannot mix addBinding() with addBindingsString()');
    }
    _bindings[key] = value;
    return this;
  }

  RequestMessageBuilder addBindings(Map<String, dynamic> bindings) {
    if (_bindingsString != null) {
      throw StateError('Cannot mix addBindings() with addBindingsString()');
    }
    _bindings.addAll(bindings);
    return this;
  }

  RequestMessageBuilder addBindingsString(String bindingsStr) {
    if (_bindings.isNotEmpty) {
      throw StateError('Cannot mix addBindingsString() with addBinding()');
    }
    _bindingsString = bindingsStr;
    return this;
  }

  RequestMessageBuilder addG(String g) {
    _g = g;
    return this;
  }

  RequestMessageBuilder addMaterializeProperties(String value) {
    if (value != RequestMessage._matTokens &&
        value != RequestMessage._matAll) {
      throw ArgumentError(
          'materializeProperties must be "tokens" or "all"');
    }
    _materializeProperties = value;
    return this;
  }

  RequestMessageBuilder addTimeoutMillis(int timeout) {
    if (timeout < 0) throw ArgumentError('timeout cannot be negative');
    _timeoutMs = timeout;
    return this;
  }

  RequestMessageBuilder addBulkResults(bool bulking) {
    _bulkResults = bulking;
    return this;
  }

  RequestMessageBuilder addField(String key, dynamic value) {
    _fields[key] = value;
    return this;
  }

  RequestMessage create() {
    String? bindings;
    if (_bindingsString != null) {
      bindings = _bindingsString;
    } else if (_bindings.isNotEmpty) {
      bindings = GremlinLang.convertParametersToString(_bindings);
    }
    return RequestMessage._(
      gremlin: _gremlin,
      language: _language,
      timeoutMs: _timeoutMs,
      bindings: bindings,
      g: _g,
      materializeProperties: _materializeProperties,
      bulkResults: _bulkResults,
      fields: Map.unmodifiable(_fields),
    );
  }
}
