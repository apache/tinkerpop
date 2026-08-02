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

import 'package:gremlin_dart/gremlin_dart.dart';

import 'cucumber_world.dart';

class ValueParser {
  final CucumberWorld world;

  ValueParser(this.world);

  dynamic parse(String literal) {
    final value = literal.trim();
    if (value == 'null') return null;
    if (value == 'true') return true;
    if (value == 'false') return false;
    if (value == 'd[NaN]') return double.nan;
    if (value == 'd[Infinity]') return double.infinity;
    if (value == 'd[-Infinity]') return double.negativeInfinity;

    final numeric = RegExp(r'^d\[(.*)\]\.([ilfdbsnm])$').firstMatch(value);
    if (numeric != null) {
      final n = numeric.group(1)!;
      switch (numeric.group(2)!) {
        case 'i':
          return GInt(int.parse(n));
        case 'l':
          return GLong(int.parse(n));
        case 'f':
          return GFloat(double.parse(n));
        case 'd':
          return GDouble(double.parse(n));
        case 'b':
          return GByte(int.parse(n));
        case 's':
          return GShort(int.parse(n));
        case 'n':
          return BigInt.parse(n);
        case 'm':
          return double.parse(n);
      }
    }

    if (_matches(value, 'str')) return _inner(value, 'str');
    if (_matches(value, 'dt')) return DateTime.parse(_inner(value, 'dt'));
    if (_matches(value, 'uuid')) return _inner(value, 'uuid');
    if (_matches(value, 'dur')) return _duration(_inner(value, 'dur'));
    if (_matches(value, 'bin')) return base64.decode(_inner(value, 'bin'));
    if (_matches(value, 'l')) return _list(_inner(value, 'l'));
    if (_matches(value, 's')) return _list(_inner(value, 's')).toSet();
    if (_matches(value, 'm')) return _map(jsonDecode(_inner(value, 'm')));
    if (_matches(value, 'p')) {
      return Path(<List<String>>[<String>[]], _list(_inner(value, 'p')));
    }
    if (_matches(value, 't')) return EnumValue('T', _inner(value, 't'));
    if (_matches(value, 'D')) return EnumValue('Direction', _inner(value, 'D'));
    if (_matches(value, 'M')) return EnumValue('Merge', _inner(value, 'M'));
    if (_matches(value, 'prop')) {
      final parts = splitTopLevel(_inner(value, 'prop'));
      return Property(parts.first, parse(parts.sublist(1).join(',')));
    }
    if (_matches(value, 'vp')) return _vertexProperty(_inner(value, 'vp'));

    final vertexId = RegExp(r'^v\[(.+)\]\.id$').firstMatch(value);
    if (vertexId != null) return _vertex(vertexId.group(1)!).id;
    final vertexStringId = RegExp(r'^v\[(.+)\]\.sid$').firstMatch(value);
    if (vertexStringId != null) return _vertex(vertexStringId.group(1)!).id.toString();
    if (_matches(value, 'v')) return _vertex(_inner(value, 'v'));

    final edgeId = RegExp(r'^e\[(.+)\]\.id$').firstMatch(value);
    if (edgeId != null) return _edge(edgeId.group(1)!).id;
    final edgeStringId = RegExp(r'^e\[(.+)\]\.sid$').firstMatch(value);
    if (edgeStringId != null) return _edge(edgeStringId.group(1)!).id.toString();
    if (_matches(value, 'e')) return _edge(_inner(value, 'e'));

    return value;
  }

  static List<String> splitTopLevel(String value) {
    final result = <String>[];
    final current = StringBuffer();
    var depth = 0;
    for (final rune in value.runes) {
      final char = String.fromCharCode(rune);
      if (char == '[' || char == '{') {
        depth++;
        current.write(char);
      } else if (char == ']' || char == '}') {
        depth--;
        current.write(char);
      } else if (char == ',' && depth == 0) {
        result.add(current.toString().trim());
        current.clear();
      } else {
        current.write(char);
      }
    }
    if (current.isNotEmpty) result.add(current.toString().trim());
    return result;
  }

  bool _matches(String value, String prefix) =>
      value.startsWith('$prefix[') && value.endsWith(']');

  String _inner(String value, String prefix) =>
      value.substring(prefix.length + 1, value.length - 1);

  Duration _duration(String value) {
    final parts = value.split(',').map((part) => part.trim()).toList();
    final seconds = int.parse(parts[0]);
    final nanos = int.parse(parts[1]);
    final positive = parts.length < 3 || parts[2] == 'true';
    final duration = Duration(seconds: seconds, microseconds: nanos ~/ 1000);
    return positive ? duration : -duration;
  }

  List<dynamic> _list(String value) {
    if (value.isEmpty) return <dynamic>[];
    return splitTopLevel(value).map(parse).toList();
  }

  dynamic _map(dynamic value) {
    if (value is List) return value.map(_map).toList();
    if (value is Map) {
      return value.map((key, val) => MapEntry(_map(key), _map(val)));
    }
    if (value is String) return parse(value);
    return value;
  }

  Vertex _vertex(String name) {
    final graph = world.graphDataMap[world.graphName];
    return graph?.vertices[name] ?? Vertex(name, 'vertex');
  }

  Edge _edge(String name) {
    final edge = world.graphDataMap[world.graphName]?.edges[name];
    if (edge == null) throw StateError('Edge with key $name not found');
    return edge;
  }

  VertexProperty _vertexProperty(String name) {
    final vp = world.graphDataMap[world.graphName]?.vertexProperties[name];
    if (vp == null) throw StateError('VertexProperty with key $name not found');
    return vp;
  }
}
