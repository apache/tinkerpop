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

import '../structure/graph.dart';
import 'gremlin_lang.dart';
import 'traversal.dart';
import 'traversal_strategy.dart';

// ---------------------------------------------------------------------------
// GraphTraversalSource
// ---------------------------------------------------------------------------

class GraphTraversalSource {
  final Graph graph;
  final TraversalStrategies traversalStrategies;
  final GremlinLang gremlinLang;

  GraphTraversalSource(
    this.graph,
    this.traversalStrategies, [
    GremlinLang? gremlinLang,
  ]) : gremlinLang = gremlinLang ?? GremlinLang();

  GraphTraversalSource _spawn([GremlinLang? gl]) => GraphTraversalSource(
        graph,
        TraversalStrategies(traversalStrategies),
        gl ?? GremlinLang(gremlinLang),
      );

  GraphTraversal _spawnTraversal(GremlinLang gl) =>
      GraphTraversal(graph, traversalStrategies, gl);

  // ---- Source modifiers ----------------------------------------------------

  GraphTraversalSource withStrategies(List<TraversalStrategy> strategies) {
    final gl = GremlinLang(gremlinLang)..addSource('withStrategies', strategies);
    return _spawn(gl);
  }

  GraphTraversalSource with_(String key, [dynamic value]) {
    final val = value ?? true;
    final opts = gremlinLang.getOptionsStrategies();
    if (opts.isEmpty) {
      return withStrategies([OptionsStrategy({key: val})]);
    }
    opts.last.configuration[key] = val;
    return _spawn(GremlinLang(gremlinLang));
  }

  GraphTraversalSource withBulk([List<dynamic>? args]) =>
      _spawn(GremlinLang(gremlinLang)..addSource('withBulk', args));

  GraphTraversalSource withPath([List<dynamic>? args]) =>
      _spawn(GremlinLang(gremlinLang)..addSource('withPath', args));

  GraphTraversalSource withSack([List<dynamic>? args]) =>
      _spawn(GremlinLang(gremlinLang)..addSource('withSack', args));

  GraphTraversalSource withSideEffect([List<dynamic>? args]) =>
      _spawn(GremlinLang(gremlinLang)..addSource('withSideEffect', args));

  // ---- Spawn traversals ----------------------------------------------------

  GraphTraversal V([List<dynamic>? args]) =>
      _spawnTraversal(GremlinLang(gremlinLang)..addStep('V', args));

  GraphTraversal E([List<dynamic>? args]) =>
      _spawnTraversal(GremlinLang(gremlinLang)..addStep('E', args));

  GraphTraversal addV([dynamic label]) =>
      _spawnTraversal(GremlinLang(gremlinLang)..addStep('addV', label != null ? [label] : null));

  GraphTraversal addE(dynamic label) =>
      _spawnTraversal(GremlinLang(gremlinLang)..addStep('addE', [label]));

  GraphTraversal mergeV([dynamic args]) =>
      _spawnTraversal(GremlinLang(gremlinLang)..addStep('mergeV', args != null ? [args] : null));

  GraphTraversal mergeE([dynamic args]) =>
      _spawnTraversal(GremlinLang(gremlinLang)..addStep('mergeE', args != null ? [args] : null));

  GraphTraversal inject(List<dynamic> args) =>
      _spawnTraversal(GremlinLang(gremlinLang)..addStep('inject', args));

  GraphTraversal io(String file) =>
      _spawnTraversal(GremlinLang(gremlinLang)..addStep('io', [file]));

  GraphTraversal call_(String procedure, [List<dynamic>? args]) =>
      _spawnTraversal(GremlinLang(gremlinLang)
        ..addStep('call', [procedure, ...?args]));

  @override
  String toString() => 'graphtraversalsource[$graph]';
}

// ---------------------------------------------------------------------------
// GraphTraversal — the core DSL
// All step methods follow the same pattern:
//   - Clone current GremlinLang
//   - Add the step
//   - Return a new GraphTraversal with same graph/strategies
// ---------------------------------------------------------------------------

class GraphTraversal extends Traversal {
  GraphTraversal(
    Graph? graph,
    TraversalStrategies? strategies,
    GremlinLang gl,
  ) : super(graph, strategies, gl);

  GraphTraversal _step(String name, [List<dynamic>? args]) =>
      GraphTraversal(graph, traversalStrategies, GremlinLang(gremlinLang)..addStep(name, args));

  // ---- Map steps -----------------------------------------------------------

  GraphTraversal map_(dynamic traversalOrLambda) =>
      _step('map', [traversalOrLambda]);

  GraphTraversal flatMap(dynamic traversalOrLambda) =>
      _step('flatMap', [traversalOrLambda]);

  GraphTraversal id() => _step('id');

  GraphTraversal label() => _step('label');

  GraphTraversal identity() => _step('identity');

  GraphTraversal constant(dynamic value) => _step('constant', [value]);

  GraphTraversal V([List<dynamic>? args]) => _step('V', args);

  GraphTraversal E([List<dynamic>? args]) => _step('E', args);

  GraphTraversal to(dynamic toVertex) => _step('to', [toVertex]);

  GraphTraversal from_(dynamic fromVertex) => _step('from', [fromVertex]);

  GraphTraversal out([List<String>? labels]) =>
      _step('out', labels?.isNotEmpty == true ? labels : null);

  GraphTraversal in_([List<String>? labels]) =>
      _step('in', labels?.isNotEmpty == true ? labels : null);

  GraphTraversal both([List<String>? labels]) =>
      _step('both', labels?.isNotEmpty == true ? labels : null);

  GraphTraversal outE([List<String>? labels]) =>
      _step('outE', labels?.isNotEmpty == true ? labels : null);

  GraphTraversal inE([List<String>? labels]) =>
      _step('inE', labels?.isNotEmpty == true ? labels : null);

  GraphTraversal bothE([List<String>? labels]) =>
      _step('bothE', labels?.isNotEmpty == true ? labels : null);

  GraphTraversal outV() => _step('outV');

  GraphTraversal inV() => _step('inV');

  GraphTraversal bothV() => _step('bothV');

  GraphTraversal otherV() => _step('otherV');

  GraphTraversal order([dynamic scope]) =>
      _step('order', scope != null ? [scope] : null);

  GraphTraversal properties([List<String>? keys]) =>
      _step('properties', keys?.isNotEmpty == true ? keys : null);

  GraphTraversal values([List<String>? keys]) =>
      _step('values', keys?.isNotEmpty == true ? keys : null);

  GraphTraversal propertyMap([List<String>? keys]) =>
      _step('propertyMap', keys?.isNotEmpty == true ? keys : null);

  GraphTraversal elementMap([List<String>? keys]) =>
      _step('elementMap', keys?.isNotEmpty == true ? keys : null);

  GraphTraversal valueMap([List<dynamic>? args]) =>
      _step('valueMap', args?.isNotEmpty == true ? args : null);

  GraphTraversal select(dynamic first, [dynamic second, dynamic third]) {
    final args = [first, if (second != null) second, if (third != null) third];
    return _step('select', args);
  }

  GraphTraversal by(dynamic arg, [dynamic order]) =>
      _step('by', [arg, if (order != null) order]);

  GraphTraversal fold([dynamic seed, dynamic foldFunction]) {
    if (seed != null && foldFunction != null) {
      return _step('fold', [seed, foldFunction]);
    }
    return _step('fold');
  }

  GraphTraversal unfold() => _step('unfold');

  GraphTraversal path() => _step('path');

  GraphTraversal limit(dynamic scopeOrCount, [int? count]) {
    if (count != null) return _step('limit', [scopeOrCount, count]);
    return _step('limit', [scopeOrCount]);
  }

  GraphTraversal tail([dynamic scopeOrCount, int? count]) {
    if (count != null) return _step('tail', [scopeOrCount, count]);
    if (scopeOrCount != null) return _step('tail', [scopeOrCount]);
    return _step('tail');
  }

  GraphTraversal range(dynamic start, dynamic end) =>
      _step('range', [start, end]);

  GraphTraversal skip(dynamic scopeOrCount, [int? count]) {
    if (count != null) return _step('skip', [scopeOrCount, count]);
    return _step('skip', [scopeOrCount]);
  }

  GraphTraversal sample(dynamic scopeOrAmount, [int? amount]) {
    if (amount != null) return _step('sample', [scopeOrAmount, amount]);
    return _step('sample', [scopeOrAmount]);
  }

  GraphTraversal count([dynamic scope]) =>
      _step('count', scope != null ? [scope] : null);

  GraphTraversal sum([dynamic scope]) =>
      _step('sum', scope != null ? [scope] : null);

  GraphTraversal max([dynamic scope]) =>
      _step('max', scope != null ? [scope] : null);

  GraphTraversal min([dynamic scope]) =>
      _step('min', scope != null ? [scope] : null);

  GraphTraversal mean([dynamic scope]) =>
      _step('mean', scope != null ? [scope] : null);

  GraphTraversal group([dynamic sideEffectKey]) =>
      _step('group', sideEffectKey != null ? [sideEffectKey] : null);

  GraphTraversal groupCount([String? sideEffectKey]) =>
      _step('groupCount', sideEffectKey != null ? [sideEffectKey] : null);

  GraphTraversal tree([String? sideEffectKey]) =>
      _step('tree', sideEffectKey != null ? [sideEffectKey] : null);

  // ---- Filter steps --------------------------------------------------------

  GraphTraversal filter(dynamic traversalOrPredicate) =>
      _step('filter', [traversalOrPredicate]);

  GraphTraversal has(dynamic first, [dynamic second, dynamic third]) {
    final args = [first, if (second != null) second, if (third != null) third];
    return _step('has', args);
  }

  GraphTraversal hasLabel(dynamic first, [List<String>? rest]) =>
      _step('hasLabel', [first, ...?rest]);

  GraphTraversal hasId(dynamic first, [List<dynamic>? rest]) =>
      _step('hasId', [first, ...?rest]);

  GraphTraversal hasKey(dynamic first, [List<String>? rest]) =>
      _step('hasKey', [first, ...?rest]);

  GraphTraversal hasValue(dynamic first, [List<dynamic>? rest]) =>
      _step('hasValue', [first, ...?rest]);

  GraphTraversal hasNot(String key) => _step('hasNot', [key]);

  GraphTraversal and_(List<dynamic> traversals) => _step('and', traversals);

  GraphTraversal or_(List<dynamic> traversals) => _step('or', traversals);

  GraphTraversal not_(dynamic traversal) => _step('not', [traversal]);

  GraphTraversal where(dynamic predicateOrTraversal) =>
      _step('where', [predicateOrTraversal]);

  GraphTraversal is_(dynamic predicateOrValue) =>
      _step('is', [predicateOrValue]);

  GraphTraversal dedup([List<dynamic>? args]) => _step('dedup', args);

  GraphTraversal simplePath() => _step('simplePath');

  GraphTraversal cyclicPath() => _step('cyclicPath');

  // ---- Side-effect steps ---------------------------------------------------

  GraphTraversal sideEffect(dynamic traversal) =>
      _step('sideEffect', [traversal]);

  GraphTraversal store(String key) => _step('store', [key]);

  GraphTraversal aggregate(dynamic first, [String? key]) =>
      _step('aggregate', [first, if (key != null) key]);

  GraphTraversal subgraph(String key) => _step('subgraph', [key]);

  GraphTraversal cap(String first, [List<String>? rest]) =>
      _step('cap', [first, ...?rest]);

  GraphTraversal timeLimit(int millis) => _step('timeLimit', [millis]);

  GraphTraversal profile([String? key]) =>
      _step('profile', key != null ? [key] : null);

  GraphTraversal property(dynamic first, dynamic second,
          [List<dynamic>? rest]) =>
      _step('property', [first, second, ...?rest]);

  // ---- Branch steps --------------------------------------------------------

  GraphTraversal branch(dynamic traversal) => _step('branch', [traversal]);

  GraphTraversal choose(dynamic first, [dynamic second, dynamic third]) {
    final args = [first, if (second != null) second, if (third != null) third];
    return _step('choose', args);
  }

  GraphTraversal optional(dynamic traversal) =>
      _step('optional', [traversal]);

  GraphTraversal union(List<dynamic> traversals) =>
      _step('union', traversals);

  GraphTraversal coalesce(List<dynamic> traversals) =>
      _step('coalesce', traversals);

  GraphTraversal repeat(dynamic traversal) => _step('repeat', [traversal]);

  GraphTraversal emit([dynamic traversalOrPredicate]) =>
      _step('emit', traversalOrPredicate != null ? [traversalOrPredicate] : null);

  GraphTraversal until(dynamic traversalOrPredicate) =>
      _step('until', [traversalOrPredicate]);

  GraphTraversal times(int count) => _step('times', [count]);

  GraphTraversal local(dynamic traversal) => _step('local', [traversal]);

  // ---- Mutation steps ------------------------------------------------------

  GraphTraversal addV_([dynamic label]) =>
      _step('addV', label != null ? [label] : null);

  GraphTraversal addE_(dynamic label) => _step('addE', [label]);

  GraphTraversal drop() => _step('drop');

  GraphTraversal mergeV_([dynamic args]) =>
      _step('mergeV', args != null ? [args] : null);

  GraphTraversal mergeE_([dynamic args]) =>
      _step('mergeE', args != null ? [args] : null);

  GraphTraversal option(dynamic first, [dynamic second]) =>
      _step('option', [first, if (second != null) second]);

  // ---- Math / string steps -------------------------------------------------

  GraphTraversal math_(String expression) => _step('math', [expression]);

  GraphTraversal concat_(List<dynamic> args) => _step('concat', args);

  GraphTraversal toLower() => _step('toLower');

  GraphTraversal toUpper() => _step('toUpper');

  GraphTraversal trim() => _step('trim');

  GraphTraversal lTrim() => _step('lTrim');

  GraphTraversal rTrim() => _step('rTrim');

  GraphTraversal length_() => _step('length');

  GraphTraversal reverse() => _step('reverse');

  GraphTraversal replace(String from, String to) =>
      _step('replace', [from, to]);

  GraphTraversal split(String separator) => _step('split', [separator]);

  GraphTraversal substring(int start, [int? end]) =>
      _step('substring', [start, if (end != null) end]);

  GraphTraversal asString() => _step('asString');

  // ---- Misc steps ----------------------------------------------------------

  GraphTraversal as_(List<String> labels) => _step('as', labels);

  GraphTraversal barrier([dynamic maxBarrierSize]) =>
      _step('barrier', maxBarrierSize != null ? [maxBarrierSize] : null);

  GraphTraversal index() => _step('index');

  GraphTraversal none_() => _step('none');

  GraphTraversal read() => _step('read');

  GraphTraversal write() => _step('write');

  GraphTraversal with_(String key, [dynamic value]) =>
      _step('with', [key, if (value != null) value]);

  GraphTraversal coin(double probability) => _step('coin', [probability]);

  GraphTraversal element() => _step('element');

  GraphTraversal discard() => _step('discard');

  GraphTraversal fail([String? message]) =>
      _step('fail', message != null ? [message] : null);

  GraphTraversal intersect_(dynamic first, [dynamic second]) =>
      _step('intersect', [first, if (second != null) second]);

  GraphTraversal any_(dynamic traversalOrPredicate) =>
      _step('any', [traversalOrPredicate]);

  GraphTraversal all_(dynamic traversalOrPredicate) =>
      _step('all', [traversalOrPredicate]);

  GraphTraversal none__(dynamic traversalOrPredicate) =>
      _step('none', [traversalOrPredicate]);

  GraphTraversal difference(dynamic first, [dynamic second]) =>
      _step('difference', [first, if (second != null) second]);

  GraphTraversal product(dynamic first, [dynamic second]) =>
      _step('product', [first, if (second != null) second]);

  GraphTraversal combine(dynamic first, [dynamic second]) =>
      _step('combine', [first, if (second != null) second]);

  GraphTraversal merge_(dynamic first, [dynamic second]) =>
      _step('merge', [first, if (second != null) second]);
}
