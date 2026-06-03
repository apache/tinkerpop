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

import 'dart:typed_data';

import 'package:gremlin_dart/gremlin_dart.dart';
import 'package:test/test.dart';

import 'cucumber_world.dart';
import 'feature_runner.dart';
import 'graph_setup.dart';
import 'value_parser.dart';

const skipTags = <String>{
  'GraphComputerOnly',
  'AllowNullPropertyValues',
  'StepTree',
  'StepWrite',
  'StepSubgraph',
  'DataChar',
  'WithReservedKeysVerificationStrategy',
  // Optional features not supported by beta.2 gremlin-lang string parser
  'SupportsDuration',
  'SupportsBinary',
  'SupportsChar',
  'SupportsBigInteger',
  'SupportsBigDecimal',
};

class FeatureSteps {
  final CucumberWorld world;
  final GraphSetup graphSetup;

  FeatureSteps(this.world) : graphSetup = GraphSetup(world.serverUrl);

  Future<void> run(FeatureScenario scenario) async {
    world.resetScenario(scenario.tags);
    world.ignore = scenario.tags.any(skipTags.contains);
    for (final step in scenario.steps) {
      await runStep(step);
    }
  }

  Future<void> runStep(FeatureStep step) async {
    final text = step.text;
    if (world.ignore) return;

    final graphMatch = RegExp(r'^the (\w+) graph$').firstMatch(text);
    if (graphMatch != null) {
      await _chooseGraph(graphMatch.group(1)!);
      return;
    }

    final parameterMatch =
        RegExp(r'^using the parameter (\w+) defined as "(.*)"$').firstMatch(text);
    if (parameterMatch != null) {
      world.params[parameterMatch.group(1)!] =
          ValueParser(world).parse(parameterMatch.group(2)!.replaceAll(r'\"', '"'));
      return;
    }

    final sideEffectMatch =
        RegExp(r'^using the side effect (\w+) defined as "(.*)"$').firstMatch(text);
    if (sideEffectMatch != null) {
      world.sideEffects[sideEffectMatch.group(1)!] =
          ValueParser(world).parse(sideEffectMatch.group(2)!.replaceAll(r'\"', '"'));
      return;
    }

    if (text == 'the traversal of') {
      world.pendingTraversal = step.docString;
      return;
    }

    if (text == 'the graph initializer of') {
      await _submit(step.docString ?? '');
      // Refresh vertex/edge lookups for empty graph so v[name].id params resolve correctly
      if (world.graphName == 'empty') {
        await graphSetup.refreshEmptyGraph(world.graphDataMap['empty']!);
      }
      return;
    }

    if (text == 'iterated to list') {
      await _iterateToList();
      return;
    }

    if (text == 'iterated next') {
      await _iterateNext();
      return;
    }

    if (text == 'the result should be empty') {
      _assertNoError();
      expect(world.result, isEmpty);
      return;
    }

    final countMatch =
        RegExp(r'^the result should have a count of (\d+)$').firstMatch(text);
    if (countMatch != null) {
      _assertNoError();
      expect(world.result.length, int.parse(countMatch.group(1)!));
      return;
    }

    if (text == 'the result should be ordered') {
      _assertTable(step.table, ordered: true);
      return;
    }

    if (text == 'the result should be unordered') {
      _assertTable(step.table, ordered: false);
      return;
    }

    if (text == 'the result should be of') {
      _assertSubset(step.table);
      return;
    }

    if (text == 'the traversal will raise an error') {
      expect(world.errorMessage, isNotNull);
      return;
    }

    final errorMsgMatch = RegExp(
      r'^the traversal will raise an error with message containing text of "(.*)"$',
    ).firstMatch(text);
    if (errorMsgMatch != null) {
      final expectedText = errorMsgMatch.group(1)!.replaceAll(r'\"', '"');
      if (world.errorMessage == null) {
        fail('Expected traversal to raise an error containing "$expectedText" but it succeeded');
      }
      expect(world.errorMessage, contains(expectedText),
          reason: 'Error message should contain "$expectedText"');
      return;
    }

    final graphCountMatch =
        RegExp(r'^the graph should return (\d+) for count of "(.*)"$')
            .firstMatch(text);
    if (graphCountMatch != null) {
      await _assertGraphCount(
        int.parse(graphCountMatch.group(1)!),
        graphCountMatch.group(2)!.replaceAll(r'\"', '"'),
      );
      return;
    }

    if (text == 'nothing should happen because' ||
        text == 'an unsupported test' ||
        text.startsWith('the file ') && text.endsWith(' should exist')) {
      world.ignore = text == 'an unsupported test';
      return;
    }

    throw UnsupportedError('Unsupported feature step: $text');
  }

  Future<void> _chooseGraph(String graphName) async {
    world.graphName = graphName;
    final graph = world.graphDataMap[graphName];
    if (graph == null) throw StateError('Unknown graph: $graphName');
    world.g = traversal().withRemote(graph.connection).with_('language', 'gremlin-lang');
    if (graphName == 'empty') {
      await graphSetup.cleanEmptyGraph();
      graph.vertices = <String, Vertex>{};
      graph.edges = <String, Edge>{};
      graph.vertexProperties = <String, VertexProperty>{};
    }
  }

  Future<void> _iterateToList() async {
    try {
      world.result = await _submit(world.pendingTraversal ?? '');
      world.errorMessage = null;
    } catch (error) {
      world.result = <dynamic>[];
      world.errorMessage = error.toString();
    }
  }

  Future<void> _iterateNext() async {
    try {
      final list = await _submit(world.pendingTraversal ?? '');
      if (list.isEmpty) {
        world.result = <dynamic>[];
      } else if (list.first is Map) {
        // group() returns a single Map; expand into entries so count == map.length
        final map = list.first as Map;
        world.result = map.entries.map((e) => MapEntry(e.key, e.value)).toList();
      } else if (list.first is Iterable && list.first is! String) {
        world.result = List<dynamic>.from(list.first as Iterable);
      } else {
        world.result = <dynamic>[list.first];
      }
      world.errorMessage = null;
    } catch (error) {
      world.result = <dynamic>[];
      world.errorMessage = error.toString();
    }
  }

  Future<List<dynamic>> _submit(String traversalString) {
    return graphSetup.submit(
      traversalString.trim(),
      graphTraversalSources[world.graphName]!,
      world.params,
      world.sideEffects,
    );
  }

  void _assertNoError() {
    if (world.errorMessage != null) {
      fail(world.errorMessage!);
    }
  }

  void _assertTable(List<Map<String, String>> table, {required bool ordered}) {
    _assertNoError();
    expect(world.result.length, table.length, reason: 'result: ${world.result}');
    final expected =
        table.map((row) => ValueParser(world).parse(row['result']!)).toList();
    if (ordered) {
      for (var i = 0; i < expected.length; i++) {
        expect(_deepEquals(world.result[i], expected[i]), isTrue,
            reason: 'expected ${expected[i]} but got ${world.result[i]}');
      }
      return;
    }

    final remaining = List<dynamic>.from(world.result);
    for (final expectedValue in expected) {
      final index =
          remaining.indexWhere((actual) => _deepEquals(actual, expectedValue));
      expect(index, isNonNegative,
          reason: 'expected $expectedValue in ${world.result}');
      remaining.removeAt(index);
    }
    expect(remaining, isEmpty);
  }

  void _assertSubset(List<Map<String, String>> table) {
    _assertNoError();
    final expected =
        table.map((row) => ValueParser(world).parse(row['result']!)).toList();
    for (final actual in world.result) {
      expect(expected.any((value) => _deepEquals(actual, value)), isTrue,
          reason: 'unexpected result $actual');
    }
  }

  Future<void> _assertGraphCount(int count, String traversalString) async {
    _assertNoError();
    final result = await _submit('$traversalString.count()');
    expect(result.single, count);
  }

  bool _deepEquals(dynamic actual, dynamic expected) {
    final a = _normalize(actual);
    final b = _normalize(expected);
    if (a is double && b is double && a.isNaN && b.isNaN) return true;
    if (a is List && b is List) {
      if (a.length != b.length) return false;
      for (var i = 0; i < a.length; i++) {
        if (!_deepEquals(a[i], b[i])) return false;
      }
      return true;
    }
    if (a is Set && b is Set) {
      if (a.length != b.length) return false;
      final remaining = b.toList();
      for (final value in a) {
        final index = remaining.indexWhere((item) => _deepEquals(value, item));
        if (index < 0) return false;
        remaining.removeAt(index);
      }
      return true;
    }
    if (a is Map && b is Map) {
      if (a.length != b.length) return false;
      for (final aEntry in a.entries) {
        // Use deep key equality (Dart's Map.containsKey uses == which fails for List keys)
        final bEntry = b.entries.where((e) => _deepEquals(e.key, aEntry.key)).firstOrNull;
        if (bEntry == null || !_deepEquals(aEntry.value, bEntry.value)) {
          return false;
        }
      }
      return true;
    }
    if (a is Uint8List && b is Uint8List) {
      if (a.length != b.length) return false;
      for (var i = 0; i < a.length; i++) {
        if (a[i] != b[i]) return false;
      }
      return true;
    }
    return a == b;
  }

  dynamic _normalize(dynamic value) {
    if (value is GInt) return value.value;
    if (value is GLong) return value.value;
    if (value is GFloat) return value.value;
    if (value is GDouble) return value.value;
    if (value is GByte) return value.value;
    if (value is GShort) return value.value;
    if (value is EnumValue) return value.toString();
    if (value is Path) return value.objects.map(_normalize).toList();
    if (value is Property) return Property(value.key, _normalize(value.value));
    if (value is VertexProperty) {
      return VertexProperty(value.id, value.label, _normalize(value.value));
    }
    if (value is List) return value.map(_normalize).toList();
    if (value is Set) return value.map(_normalize).toSet();
    if (value is Map) {
      return value.map((key, val) => MapEntry(_normalize(key), _normalize(val)));
    }
    return value;
  }
}
