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

import 'package:antlr4/antlr4.dart';

import '../../process/graph_traversal.dart';
import '../../process/gremlin_lang.dart';
import '../../process/traversal.dart';
import '../../process/traversal_strategy.dart';
import 'GremlinLexer.dart';
import 'GremlinParser.dart';

class GremlinAntlrToDart {
  static const _spawnMethods = <String>{
    'V',
    'E',
    'addV',
    'addE',
    'mergeV',
    'mergeE',
    'inject',
    'io',
    'call',
    'union',
  };

  static const _terminalMethods = <String>{'toList', 'next', 'iterate'};

  final GraphTraversalSource g;
  final Map<String, dynamic> variables;

  GremlinAntlrToDart(this.g, {Map<String, dynamic>? variables})
      : variables = Map<String, dynamic>.from(variables ?? const {});

  static dynamic parse(
    GraphTraversalSource g,
    String script, {
    Map<String, dynamic>? variables,
  }) {
    return GremlinAntlrToDart(g, variables: variables).parseScript(script);
  }

  dynamic parseScript(String script) {
    final lexer = GremlinLexer(InputStream.fromString(script));
    final parser = GremlinParser(CommonTokenStream(lexer));
    final query = parser.query();
    final normalized = query.text;
    if (normalized.isEmpty) return '';
    return _parseNormalized(normalized);
  }

  dynamic _parseNormalized(String script) {
    final parts = _splitChain(script);
    if (parts.isEmpty || parts.first != 'g') {
      throw UnsupportedError('Expected traversal source "g": $script');
    }

    var source = g;
    var index = 1;
    while (index < parts.length) {
      final call = _parseCall(parts[index]);
      if (_spawnMethods.contains(call.name)) break;
      source = _applySourceCall(source, call);
      index++;
    }

    if (index >= parts.length) return source;

    var traversal = _spawnTraversal(source, _parseCall(parts[index]));
    index++;

    while (index < parts.length) {
      final call = _parseCall(parts[index]);
      if (_terminalMethods.contains(call.name) && index == parts.length - 1) {
        return _applyTerminal(traversal, call);
      }
      traversal = _appendStep(traversal, call.name, _parseArgs(call.argsText));
      index++;
    }

    return traversal;
  }

  GraphTraversalSource _applySourceCall(
    GraphTraversalSource source,
    _Call call,
  ) {
    switch (call.name) {
      case 'with':
        final args = _parseArgs(call.argsText);
        return args.length == 1 ? source.with_(args[0] as String) : source.with_(args[0] as String, args[1]);
      case 'withBulk':
        return source.withBulk(_parseArgs(call.argsText));
      case 'withPath':
        return source.withPath(_parseArgs(call.argsText));
      case 'withSack':
        return source.withSack(_parseArgs(call.argsText));
      case 'withSideEffect':
        return source.withSideEffect(_parseArgs(call.argsText));
      case 'withStrategies':
        return source.withStrategies(
          _splitArgs(call.argsText).map(_parseStrategy).toList(),
        );
      case 'withoutStrategies':
        return source.withoutStrategies(
          _splitArgs(call.argsText).map((name) => name.replaceFirst(RegExp(r'^new'), '').trim()).toList(),
        );
      default:
        throw UnsupportedError('Unsupported traversal source method: ${call.name}');
    }
  }

  GraphTraversal _spawnTraversal(GraphTraversalSource source, _Call call) {
    final args = _parseArgs(call.argsText);
    switch (call.name) {
      case 'V':
        return source.V(args);
      case 'E':
        return source.E(args);
      case 'addV':
        return source.addV(args.isEmpty ? null : args.first);
      case 'addE':
        return source.addE(args.first);
      case 'mergeV':
        return source.mergeV(args.isEmpty ? null : args.first);
      case 'mergeE':
        return source.mergeE(args.isEmpty ? null : args.first);
      case 'inject':
        return source.inject(args);
      case 'io':
        return source.io(args.first as String);
      case 'call':
        return source.call_(
          args.isEmpty ? '' : args.first as String,
          args.length <= 1 ? null : args.sublist(1),
        );
      case 'union':
        return _appendStep(
          GraphTraversal(
            source.graph,
            source.traversalStrategies,
            GremlinLang(source.gremlinLang),
          ),
          'union',
          args,
        );
      default:
        throw UnsupportedError('Unsupported traversal spawn method: ${call.name}');
    }
  }

  dynamic _applyTerminal(GraphTraversal traversal, _Call call) {
    switch (call.name) {
      case 'toList':
        return traversal.toList();
      case 'next':
        return traversal.next();
      case 'iterate':
        return traversal.iterate();
      default:
        throw UnsupportedError('Unsupported terminal method: ${call.name}');
    }
  }

  GraphTraversal _appendStep(
    GraphTraversal traversal,
    String name,
    List<dynamic> args,
  ) {
    return GraphTraversal(
      traversal.graph,
      traversal.traversalStrategies,
      GremlinLang(traversal.gremlinLang)..addStep(name, args.isEmpty ? null : args),
    );
  }

  List<String> _splitChain(String script) {
    final result = <String>[];
    final current = StringBuffer();
    var depth = 0;
    String? quote;
    var escaped = false;

    for (final rune in script.runes) {
      final ch = String.fromCharCode(rune);
      if (escaped) {
        current.write(ch);
        escaped = false;
        continue;
      }

      if (quote != null) {
        current.write(ch);
        if (ch == r'\') {
          escaped = true;
        } else if (ch == quote) {
          quote = null;
        }
        continue;
      }

      if (ch == '"' || ch == "'") {
        quote = ch;
        current.write(ch);
        continue;
      }

      if (ch == '(' || ch == '[' || ch == '{') depth++;
      if (ch == ')' || ch == ']' || ch == '}') depth--;

      if (ch == '.' && depth == 0) {
        result.add(current.toString());
        current.clear();
      } else {
        current.write(ch);
      }
    }

    if (current.isNotEmpty) result.add(current.toString());
    return result;
  }

  _Call _parseCall(String segment) {
    final open = segment.indexOf('(');
    if (open == -1 || !segment.endsWith(')')) {
      return _Call(segment, '');
    }
    return _Call(segment.substring(0, open), segment.substring(open + 1, segment.length - 1));
  }

  List<String> _splitArgs(String argsText) {
    if (argsText.trim().isEmpty) return const [];

    final result = <String>[];
    final current = StringBuffer();
    var depth = 0;
    String? quote;
    var escaped = false;

    for (final rune in argsText.runes) {
      final ch = String.fromCharCode(rune);
      if (escaped) {
        current.write(ch);
        escaped = false;
        continue;
      }

      if (quote != null) {
        current.write(ch);
        if (ch == r'\') {
          escaped = true;
        } else if (ch == quote) {
          quote = null;
        }
        continue;
      }

      if (ch == '"' || ch == "'") {
        quote = ch;
        current.write(ch);
        continue;
      }

      if (ch == '(' || ch == '[' || ch == '{') depth++;
      if (ch == ')' || ch == ']' || ch == '}') depth--;

      if (ch == ',' && depth == 0) {
        result.add(current.toString().trim());
        current.clear();
      } else {
        current.write(ch);
      }
    }

    if (current.isNotEmpty) result.add(current.toString().trim());
    return result;
  }

  List<dynamic> _parseArgs(String argsText) =>
      _splitArgs(argsText).map(_parseValue).toList();

  dynamic _parseValue(String text) {
    if (text.isEmpty) return null;
    if (text == 'null') return null;
    if (text == 'true') return true;
    if (text == 'false') return false;
    if (text == 'NaN') return double.nan;
    if (text == '+Infinity') return double.infinity;
    if (text == '-Infinity') return double.negativeInfinity;

    if (_isQuoted(text)) return _unquote(text);

    if (variables.containsKey(text)) return variables[text];

    if (text.startsWith('datetime(') ||
        text.startsWith('UUID(') ||
        text.startsWith('Duration(') ||
        text.startsWith('Binary(') ||
        text.startsWith('BigDecimal(') ||
        text.startsWith('datetime(')) {
      return GremlinRawLiteral(text);
    }

    if (text.startsWith('[') && text.endsWith(']')) {
      final inner = text.substring(1, text.length - 1);
      if (inner.trim().isEmpty) return <dynamic>[];
      if (_looksLikeMap(inner)) {
        final result = <dynamic, dynamic>{};
        for (final part in _splitArgs(inner)) {
          final idx = _indexOfTopLevel(part, ':');
          result[_parseValue(part.substring(0, idx))] =
              _parseValue(part.substring(idx + 1));
        }
        return result;
      }
      return _splitArgs(inner).map(_parseValue).toList();
    }

    if (text.startsWith('{') && text.endsWith('}')) {
      final inner = text.substring(1, text.length - 1);
      if (inner.trim().isEmpty) return <dynamic>{};
      return _splitArgs(inner).map(_parseValue).toSet();
    }

    if (_looksLikeTraversal(text)) return _parseAnonymousTraversal(text);
    if (_looksLikePredicate(text)) return _parsePredicate(text);
    if (_looksLikeStrategy(text)) return _parseStrategy(text);

    final enumValue = _parseEnum(text);
    if (enumValue != null) return enumValue;

    final number = _parseNumber(text);
    if (number != null) return number;

    if (text == '[]') return <dynamic>[];
    if (text == '[:]') return <dynamic, dynamic>{};

    return GremlinRawLiteral(text);
  }

  bool _looksLikeMap(String text) => _indexOfTopLevel(text, ':') != -1;

  int _indexOfTopLevel(String text, String needle) {
    var depth = 0;
    String? quote;
    var escaped = false;
    for (var i = 0; i < text.length; i++) {
      final ch = text[i];
      if (escaped) {
        escaped = false;
        continue;
      }
      if (quote != null) {
        if (ch == r'\') {
          escaped = true;
        } else if (ch == quote) {
          quote = null;
        }
        continue;
      }
      if (ch == '"' || ch == "'") {
        quote = ch;
        continue;
      }
      if (ch == '(' || ch == '[' || ch == '{') depth++;
      if (ch == ')' || ch == ']' || ch == '}') depth--;
      if (depth == 0 && ch == needle) return i;
    }
    return -1;
  }

  bool _looksLikeTraversal(String text) {
    if (text.startsWith('__.')) return true;
    if (!text.contains('(')) return false;
    final name = text.substring(0, text.indexOf('('));
    return RegExp(r'^[A-Za-z_][A-Za-z0-9_]*$').hasMatch(name);
  }

  GraphTraversal _parseAnonymousTraversal(String text) {
    final chain = text.startsWith('__.') ? text.substring(3) : text;
    final parts = _splitChain(chain);
    if (parts.isEmpty) {
      throw StateError('Anonymous traversal is empty: $text');
    }

    var traversal = GraphTraversal(null, null, GremlinLang());
    var initialized = false;
    for (final part in parts) {
      final call = _parseCall(part);
      final next = GraphTraversal(
        traversal.graph,
        traversal.traversalStrategies,
        GremlinLang(initialized ? traversal.gremlinLang : null)
          ..addStep(call.name, _parseArgs(call.argsText)),
      );
      traversal = next;
      initialized = true;
    }
    return traversal;
  }

  bool _looksLikePredicate(String text) =>
      text.startsWith('P.') ||
      text.startsWith('TextP.') ||
      RegExp(r'^(eq|neq|gt|gte|lt|lte|between|inside|outside|within|without|containing|notContaining|startingWith|notStartingWith|endingWith|notEndingWith|regex|notRegex)\(')
          .hasMatch(text);

  dynamic _parsePredicate(String text) {
    if (text.contains('.and(') || text.contains('.or(') || text.startsWith('not(')) {
      return GremlinRawLiteral(text);
    }

    final plain = text.replaceFirst(RegExp(r'^(P|TextP)\.'), '');
    final call = _parseCall(plain);
    final args = _parseArgs(call.argsText);

    switch (call.name) {
      case 'eq':
        return P.eq(args[0]);
      case 'neq':
        return P.neq(args[0]);
      case 'gt':
        return P.gt(args[0]);
      case 'gte':
        return P.gte(args[0]);
      case 'lt':
        return P.lt(args[0]);
      case 'lte':
        return P.lte(args[0]);
      case 'between':
        return P.between(args[0], args[1]);
      case 'inside':
        return P.inside(args[0], args[1]);
      case 'outside':
        return P.outside(args[0], args[1]);
      case 'within':
        return P.within(args);
      case 'without':
        return P.without(args);
      case 'containing':
        return TextP.containing(args[0] as String);
      case 'notContaining':
        return TextP.notContaining(args[0] as String);
      case 'startingWith':
        return TextP.startingWith(args[0] as String);
      case 'notStartingWith':
        return TextP.notStartingWith(args[0] as String);
      case 'endingWith':
        return TextP.endingWith(args[0] as String);
      case 'notEndingWith':
        return TextP.notEndingWith(args[0] as String);
      case 'regex':
        return TextP.regex(args[0] as String);
      case 'notRegex':
        return TextP.notRegex(args[0] as String);
      default:
        return GremlinRawLiteral(text);
    }
  }

  bool _looksLikeStrategy(String text) =>
      RegExp(r'^(new)?[A-Z][A-Za-z0-9_]*Strategy(\(|$)').hasMatch(text) ||
      text == 'ReadOnlyStrategy' ||
      text == 'OptionsStrategy' ||
      text == 'SeedStrategy';

  TraversalStrategy _parseStrategy(String text) {
    final trimmed = text.replaceFirst(RegExp(r'^new'), '');
    final call = _parseCall(trimmed);
    final config = <String, dynamic>{};
    for (final part in _splitArgs(call.argsText)) {
      final idx = _indexOfTopLevel(part, ':');
      if (idx == -1) continue;
      config[part.substring(0, idx).trim()] = _parseValue(part.substring(idx + 1).trim());
    }

    switch (call.name) {
      case 'OptionsStrategy':
        return OptionsStrategy(config);
      case 'PartitionStrategy':
        return PartitionStrategy(
          partitionKey: config['partitionKey'] as String?,
          writePartition: config['writePartition'] as String?,
          readPartitions: (config['readPartitions'] as List?)?.cast<String>(),
          includeMetaProperties: config['includeMetaProperties'] as bool?,
        );
      case 'SubgraphStrategy':
        return SubgraphStrategy(
          vertices: config['vertices'],
          edges: config['edges'],
          vertexProperties: config['vertexProperties'],
          checkAdjacentVertices: config['checkAdjacentVertices'] as bool?,
        );
      case 'SeedStrategy':
        return SeedStrategy(seed: (config['seed'] as num?)?.toInt() ?? 0);
      case 'ReadOnlyStrategy':
        return ReadOnlyStrategy();
      case 'VertexProgramStrategy':
        return VertexProgramStrategy(config);
      default:
        return _NamedTraversalStrategy(call.name, config);
    }
  }

  EnumValue? _parseEnum(String text) {
    final dot = text.indexOf('.');
    if (dot == -1) return switch (text) {
      'asc' => EnumValue('Order', 'asc'),
      'desc' => EnumValue('Order', 'desc'),
      'shuffle' => EnumValue('Order', 'shuffle'),
      'sum' => EnumValue('Operator', 'sum'),
      'sumLong' => EnumValue('Operator', 'sumLong'),
      'minus' => EnumValue('Operator', 'minus'),
      'mult' => EnumValue('Operator', 'mult'),
      'div' => EnumValue('Operator', 'div'),
      'min' => EnumValue('Operator', 'min'),
      'max' => EnumValue('Operator', 'max'),
      'assign' => EnumValue('Operator', 'assign'),
      'and' => EnumValue('Operator', 'and'),
      'or' => EnumValue('Operator', 'or'),
      'addAll' => EnumValue('Operator', 'addAll'),
      _ => null,
    };

    final type = text.substring(0, dot);
    final member = text.substring(dot + 1);
    return switch (type) {
      'T' => EnumValue('T', member),
      'Order' => EnumValue('Order', member),
      'Scope' => EnumValue('Scope', member),
      'Column' => EnumValue('Column', member),
      'Cardinality' => EnumValue('Cardinality', member),
      'Merge' => EnumValue('Merge', member),
      'Direction' => EnumValue('Direction', member.toUpperCase()),
      'Operator' => EnumValue('Operator', member),
      'Barrier' => EnumValue('Barrier', member),
      'Pop' => EnumValue('Pop', member),
      'Pick' => EnumValue('Pick', member),
      'DT' => EnumValue('DT', member),
      _ => null,
    };
  }

  dynamic _parseNumber(String text) {
    if (!RegExp(r'^[-+]?\d').hasMatch(text)) return null;
    final lower = text.toLowerCase();
    if (lower.endsWith('b')) return GByte(int.parse(text.substring(0, text.length - 1)));
    if (lower.endsWith('s')) return GShort(int.parse(text.substring(0, text.length - 1)));
    if (lower.endsWith('i')) return GInt(int.parse(text.substring(0, text.length - 1)));
    if (lower.endsWith('l')) return GLong(int.parse(text.substring(0, text.length - 1)));
    if (lower.endsWith('n')) return BigInt.parse(text.substring(0, text.length - 1));
    if (lower.endsWith('f')) return GFloat(double.parse(text.substring(0, text.length - 1)));
    if (lower.endsWith('d')) return GDouble(double.parse(text.substring(0, text.length - 1)));
    if (lower.endsWith('m')) return GremlinRawLiteral(text);
    if (text.contains('.') || text.contains('E') || text.contains('e')) return double.parse(text);
    return int.parse(text);
  }

  bool _isQuoted(String text) =>
      text.length >= 2 &&
      ((text.startsWith("'") && text.endsWith("'")) ||
          (text.startsWith('"') && text.endsWith('"')));

  String _unquote(String text) {
    final inner = text.substring(1, text.length - 1);
    return inner
        .replaceAll(r'\\', r'\')
        .replaceAll(r"\'", "'")
        .replaceAll(r'\"', '"')
        .replaceAll(r'\n', '\n')
        .replaceAll(r'\r', '\r')
        .replaceAll(r'\t', '\t');
  }
}

class _Call {
  final String name;
  final String argsText;

  const _Call(this.name, this.argsText);
}

class _NamedTraversalStrategy extends TraversalStrategy {
  _NamedTraversalStrategy(String name, Map<String, dynamic> configuration)
      : super(strategyName: name, configuration: configuration);
}
