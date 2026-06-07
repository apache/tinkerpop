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

import 'dart:convert';
import 'dart:typed_data';

import '../structure/graph.dart';
import 'traversal.dart';
import 'traversal_strategy.dart';

const int _int32Min = -2147483648;
const int _int32Max = 2147483647;

class GremlinLang {
  String _gremlin = '';
  final List<OptionsStrategy> _optionsStrategies;

  GremlinLang([GremlinLang? toClone])
      : _gremlin = toClone?._gremlin ?? '',
        _optionsStrategies = List.of(toClone?._optionsStrategies ?? []);

  List<OptionsStrategy> getOptionsStrategies() => _optionsStrategies;

  // -------------------------------------------------------------------------
  // Serialisation helpers
  // -------------------------------------------------------------------------

  String _predicateAsString(dynamic p) {
    if (p is P) {
      if (p.operator == 'and' || p.operator == 'or') {
        return '${_predicateAsString(p.value)}.${p.operator}(${_predicateAsString(p.other)})';
      }
      final buf = StringBuffer('${p.operator}(');
      if (p.value is List) {
        buf.write('[');
        buf.write((p.value as List).map(_argAsString).join(','));
        buf.write(']');
      } else {
        buf.write(_argAsString(p.value));
        if (p.other != null) {
          buf.write(',${_argAsString(p.other)}');
        }
      }
      buf.write(')');
      return buf.toString();
    }
    if (p is TextP) {
      final buf = StringBuffer('${p.operator}(');
      buf.write(_argAsString(p.value));
      if (p.other != null) {
        buf.write(',${_argAsString(p.other)}');
      }
      buf.write(')');
      return buf.toString();
    }
    throw ArgumentError('Expected P or TextP, got ${p.runtimeType}');
  }

  String _argAsString(dynamic arg) {
    if (arg == null) return 'null';
    if (arg is bool) return arg ? 'true' : 'false';

    if (arg is GLong) return '${arg.value}L';
    if (arg is GFloat) return _fpAsString(arg.value, 'F');
    if (arg is GDouble) return _fpAsString(arg.value, 'D');
    if (arg is GShort) return '${arg.value}S';
    if (arg is GByte) return '${arg.value}B';
    if (arg is GInt) return '${arg.value}';

    if (arg is int) {
      if (arg >= _int32Min && arg <= _int32Max) return '$arg';
      return '${arg}L';
    }
    if (arg is BigInt) return '${arg}N';
    if (arg is double) {
      if (arg.isNaN) return 'NaN';
      if (arg.isInfinite) return arg > 0 ? '+Infinity' : '-Infinity';
      return _fpAsString(arg, 'D');
    }

    if (arg is DateTime) {
      return 'datetime("${arg.toUtc().toIso8601String()}")';
    }

    if (arg is String) {
      final escaped = arg
          .replaceAll(r'\', r'\\')
          .replaceAll("'", r"\'")
          .replaceAll('\n', r'\n')
          .replaceAll('\r', r'\r')
          .replaceAll('\t', r'\t');
      return "'$escaped'";
    }

    if (arg is P || arg is TextP) return _predicateAsString(arg);

    if (arg is EnumValue) return arg.toString();

    if (arg is TraversalStrategy && arg is! OptionsStrategy) {
      final name = arg.strategyName;
      final entries = arg.configuration.entries.toList();
      if (entries.isEmpty) return name;
      final cfg =
          entries.map((e) => '${e.key}:${_argAsString(e.value)}').join(',');
      return 'new $name($cfg)';
    }

    if (arg is Vertex) return _argAsString(arg.id);

    if (arg is GremlinLang) return arg.getGremlin('__');

    if (arg is Traversal) {
      if (arg.graph != null) {
        throw StateError(
            'Child traversal must be anonymous - use __ not g');
      }
      return arg.getGremlinLang().getGremlin('__');
    }

    if (arg is Uint8List) {
      return 'Binary("${base64.encode(arg)}")';
    }

    if (arg is Set) {
      if (arg.isEmpty) return '{}';
      return '{${arg.map(_argAsString).join(',')}}';
    }

    if (arg is Map) {
      if (arg.isEmpty) return '[:]';
      final parts = <String>[];
      arg.forEach((k, v) {
        final ks = _argAsString(k);
        final vs = _argAsString(v);
        parts.add(k is String ? '$ks:$vs' : '($ks):$vs');
      });
      return '[${parts.join(',')}]';
    }

    if (arg is List) {
      return '[${arg.map(_argAsString).join(',')}]';
    }

    throw ArgumentError(
        'GremlinLang cannot represent type ${arg.runtimeType}');
  }

  static String _fpAsString(double v, String suffix) {
    if (v.isNaN) return 'NaN';
    if (v.isInfinite) return v > 0 ? '+Infinity' : '-Infinity';
    if (v == v.truncateToDouble()) return '${v.toStringAsFixed(1)}$suffix';
    return '$v$suffix';
  }

  // -------------------------------------------------------------------------
  // Step / source builders
  // -------------------------------------------------------------------------

  GremlinLang addStep(String name, [List<dynamic>? args]) {
    final argsStr = args != null && args.isNotEmpty
        ? args.map(_argAsString).join(',')
        : '';
    _gremlin += '.$name($argsStr)';
    return this;
  }

  GremlinLang addSource(String name, [List<dynamic>? args]) {
    if (name == 'CardinalityValueTraversal' && args != null) {
      final card = args[0] as EnumValue;
      _gremlin += 'Cardinality.${card.elementName}(${_argAsString(args[1])})';
      return this;
    }
    if (name == 'withStrategies' && args != null) {
      final nonOptions = <dynamic>[];
      for (final s in args) {
        if (s is OptionsStrategy) {
          _optionsStrategies.add(s);
        } else {
          nonOptions.add(s);
        }
      }
      if (nonOptions.isNotEmpty) {
        final argsStr = nonOptions.map(_argAsString).join(',');
        _gremlin += '.withStrategies($argsStr)';
      }
      return this;
    }
    final argsStr = args != null && args.isNotEmpty
        ? args.map(_argAsString).join(',')
        : '';
    _gremlin += '.$name($argsStr)';
    return this;
  }

  // -------------------------------------------------------------------------
  // Accessors
  // -------------------------------------------------------------------------

  String getGremlin([String prefix = 'g']) {
    if (_gremlin.isNotEmpty && !_gremlin.startsWith('.')) {
      return _gremlin;
    }
    return '$prefix$_gremlin';
  }

  /// Converts a single value to its gremlin-lang literal representation.
  static String valueToGremlinLiteral(dynamic value) =>
      GremlinLang()._argAsString(value);
}
