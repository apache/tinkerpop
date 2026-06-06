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

class RequestMessage {
  final String gremlin;
  final String language;
  final int? timeoutMs;
  final Map<String, dynamic>? bindings;
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
    _bindings[key] = value;
    return this;
  }

  RequestMessageBuilder addBindings(Map<String, dynamic> bindings) {
    _bindings.addAll(bindings);
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
    final Map<String, dynamic>? bindings =
        _bindings.isNotEmpty ? Map.unmodifiable(_bindings) : null;
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
