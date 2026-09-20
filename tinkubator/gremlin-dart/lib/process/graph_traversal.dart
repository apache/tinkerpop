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

import '../driver/transaction.dart';
import '../structure/graph.dart';
import 'gremlin_lang.dart';
import 'traversal.dart';
import 'traversal_strategy.dart';

const Object _unspecified = Object();

// ---------------------------------------------------------------------------
// GraphTraversalSource
// ---------------------------------------------------------------------------

class GraphTraversalSource {
  final Graph graph;
  final TraversalStrategies traversalStrategies;
  final GremlinLang gremlinLang;
  final TransactionCapableRemoteConnectionBase? remoteConnection;
  final String? traversalSource;

  GraphTraversalSource(
    this.graph,
    this.traversalStrategies, [
    GremlinLang? gremlinLang,
    this.remoteConnection,
    this.traversalSource,
  ]) : gremlinLang = gremlinLang ?? GremlinLang();

  GraphTraversalSource _spawn([GremlinLang? gl]) => GraphTraversalSource(
        graph,
        TraversalStrategies(traversalStrategies),
        gl ?? GremlinLang(gremlinLang),
        remoteConnection,
        traversalSource,
      );

  GraphTraversal _spawnTraversal(GremlinLang gl) =>
      GraphTraversal(graph, traversalStrategies, gl);

  // ---- Source modifiers ----------------------------------------------------

  GraphTraversalSource withStrategies(List<TraversalStrategy> strategies) {
    final gl = GremlinLang(gremlinLang)
      ..addSource('withStrategies', strategies);
    return _spawn(gl);
  }

  /// Removes named strategies from the traversal compilation pipeline.
  ///
  /// Strategy names should match the simple class name used by the server
  /// (e.g. `'ConnectiveStrategy'`, `'ReadOnlyStrategy'`).
  GraphTraversalSource withoutStrategies(List<String> strategyNames) {
    final gl = GremlinLang(gremlinLang)
      ..addSource('withoutStrategies', strategyNames);
    return _spawn(gl);
  }

  GraphTraversalSource with_(String key, [dynamic value = _unspecified]) {
    final val = identical(value, _unspecified) ? true : value;
    final gl = GremlinLang(gremlinLang);
    final opts = gl.getOptionsStrategies();
    if (opts.isEmpty) {
      opts.add(OptionsStrategy({key: val}));
    } else {
      // Replace the last entry in the clone's list with a new merged copy,
      // so the original source's OptionsStrategy object is never mutated.
      final merged = Map<String, dynamic>.from(opts.last.configuration)
        ..[key] = val;
      opts[opts.length - 1] = OptionsStrategy(merged);
    }
    return _spawn(gl);
  }

  GraphTraversalSource withBulk(
          [dynamic first = _unspecified, dynamic second = _unspecified]) =>
      _spawn(GremlinLang(gremlinLang)
        ..addSource('withBulk', _normalizeVarArgs([first, second])));

  GraphTraversalSource withPath(
          [dynamic first = _unspecified, dynamic second = _unspecified]) =>
      _spawn(GremlinLang(gremlinLang)
        ..addSource('withPath', _normalizeVarArgs([first, second])));

  GraphTraversalSource withSack(
          [dynamic first = _unspecified, dynamic second = _unspecified]) =>
      _spawn(GremlinLang(gremlinLang)
        ..addSource('withSack', _normalizeLiteralVarArgs([first, second])));

  GraphTraversalSource withSideEffect(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified]) =>
      _spawn(GremlinLang(gremlinLang)
        ..addSource(
            'withSideEffect', _normalizeVarArgs([first, second, third])));

  // ---- Spawn traversals ----------------------------------------------------

  GraphTraversal V(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified]) =>
      _spawnTraversal(GremlinLang(gremlinLang)
        ..addStep('V', _normalizeVarArgs([first, second, third])));

  GraphTraversal E(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified]) =>
      _spawnTraversal(GremlinLang(gremlinLang)
        ..addStep('E', _normalizeVarArgs([first, second, third])));

  GraphTraversal addV([dynamic label]) =>
      _spawnTraversal(GremlinLang(gremlinLang)
        ..addStep('addV', label != null ? [label] : null));

  GraphTraversal addE(dynamic label) =>
      _spawnTraversal(GremlinLang(gremlinLang)..addStep('addE', [label]));

  GraphTraversal mergeV([dynamic args = _unspecified]) =>
      _spawnTraversal(GremlinLang(gremlinLang)
        ..addStep('mergeV', args != _unspecified ? [args] : null));

  GraphTraversal mergeE([dynamic args = _unspecified]) =>
      _spawnTraversal(GremlinLang(gremlinLang)
        ..addStep('mergeE', args != _unspecified ? [args] : null));

  GraphTraversal inject([
    dynamic a = _unspecified,
    dynamic b = _unspecified,
    dynamic c = _unspecified,
    dynamic d = _unspecified,
    dynamic e = _unspecified,
    dynamic f = _unspecified,
    dynamic g = _unspecified,
    dynamic h = _unspecified,
    dynamic i = _unspecified,
    dynamic j = _unspecified,
    dynamic k = _unspecified,
    dynamic l = _unspecified,
    dynamic m = _unspecified,
    dynamic n = _unspecified,
    dynamic o = _unspecified,
    dynamic p = _unspecified,
    dynamic q = _unspecified,
    dynamic r = _unspecified,
    dynamic s = _unspecified,
    dynamic t = _unspecified,
    dynamic u = _unspecified,
    dynamic v = _unspecified,
    dynamic w = _unspecified,
    dynamic x = _unspecified,
    dynamic y = _unspecified,
    dynamic z = _unspecified,
    dynamic aa = _unspecified,
    dynamic ab = _unspecified,
    dynamic ac = _unspecified,
    dynamic ad = _unspecified,
  ]) =>
      _spawnTraversal(GremlinLang(gremlinLang)
        ..addStep(
            'inject',
            _normalizeLiteralVarArgs([
              a,
              b,
              c,
              d,
              e,
              f,
              g,
              h,
              i,
              j,
              k,
              l,
              m,
              n,
              o,
              p,
              q,
              r,
              s,
              t,
              u,
              v,
              w,
              x,
              y,
              z,
              aa,
              ab,
              ac,
              ad
            ])));

  GraphTraversal io(String file) =>
      _spawnTraversal(GremlinLang(gremlinLang)..addStep('io', [file]));

  GraphTraversal call_(String procedure, [List<dynamic>? args]) =>
      _spawnTraversal(
          GremlinLang(gremlinLang)..addStep('call', [procedure, ...?args]));

  GraphTraversal call([dynamic procedure, dynamic traversal]) =>
      _spawnTraversal(GremlinLang(gremlinLang)
        ..addStep(
            'call',
            procedure == null
                ? null
                : [procedure, if (traversal != null) traversal]));

  GraphTraversal union(
          [dynamic first, dynamic second, dynamic third, dynamic fourth]) =>
      _spawnTraversal(GremlinLang(gremlinLang)
        ..addStep(
            'union',
            first == null
                ? null
                : GraphTraversal._toTraversalList(
                    first, second, third, fourth)));

  Transaction tx() {
    if (remoteConnection == null) {
      throw StateError('Transactions require a remote traversal source');
    }
    return remoteConnection!.tx(traversalSource);
  }

  @override
  String toString() => 'graphtraversalsource[$graph]';
}

List<dynamic>? _normalizeVarArgs(List<dynamic> args) {
  final values = List<dynamic>.from(args);
  while (values.isNotEmpty && identical(values.last, _unspecified)) {
    values.removeLast();
  }
  if (values.isEmpty) return null;
  return values.length == 1 && values.first is List
      ? values.first as List<dynamic>
      : values;
}

List<dynamic>? _normalizeLiteralVarArgs(List<dynamic> args) {
  final values = List<dynamic>.from(args);
  while (values.isNotEmpty && identical(values.last, _unspecified)) {
    values.removeLast();
  }
  return values.isEmpty ? null : values;
}

// ---------------------------------------------------------------------------
// GraphTraversal — the core DSL
// All step methods follow the same pattern:
//   - Clone current GremlinLang
//   - Add the step
//   - Return a new GraphTraversal with same graph/strategies
// ---------------------------------------------------------------------------

class GraphTraversal extends Traversal {
  GraphTraversal(super.graph, super.strategies, super.gl);

  GraphTraversal _step(String name, [List<dynamic>? args]) => GraphTraversal(
      graph,
      traversalStrategies,
      GremlinLang(gremlinLang)..addStep(name, args));

  static List<dynamic> _toTraversalList(dynamic first,
      [dynamic second,
      dynamic third,
      dynamic fourth,
      dynamic fifth,
      dynamic sixth]) {
    if (first is List) return first;
    return [
      first,
      if (second != null) second,
      if (third != null) third,
      if (fourth != null) fourth,
      if (fifth != null) fifth,
      if (sixth != null) sixth,
    ];
  }

  // ---- Map steps -----------------------------------------------------------

  GraphTraversal map_(dynamic traversalOrLambda) =>
      _step('map', [traversalOrLambda]);

  GraphTraversal flatMap(dynamic traversalOrLambda) =>
      _step('flatMap', [traversalOrLambda]);

  GraphTraversal id() => _step('id');

  GraphTraversal label() => _step('label');

  GraphTraversal identity() => _step('identity');

  GraphTraversal constant(dynamic value) => _step('constant', [value]);

  GraphTraversal V(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified]) =>
      _step('V', _normalizeVarArgs([first, second, third]));

  GraphTraversal E(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified]) =>
      _step('E', _normalizeVarArgs([first, second, third]));

  GraphTraversal inject([
    dynamic a = _unspecified,
    dynamic b = _unspecified,
    dynamic c = _unspecified,
    dynamic d = _unspecified,
    dynamic e = _unspecified,
    dynamic f = _unspecified,
    dynamic g = _unspecified,
    dynamic h = _unspecified,
    dynamic i = _unspecified,
    dynamic j = _unspecified,
    dynamic k = _unspecified,
    dynamic l = _unspecified,
    dynamic m = _unspecified,
    dynamic n = _unspecified,
    dynamic o = _unspecified,
    dynamic p = _unspecified,
    dynamic q = _unspecified,
    dynamic r = _unspecified,
    dynamic s = _unspecified,
    dynamic t = _unspecified,
    dynamic u = _unspecified,
    dynamic v = _unspecified,
    dynamic w = _unspecified,
    dynamic x = _unspecified,
    dynamic y = _unspecified,
    dynamic z = _unspecified,
    dynamic aa = _unspecified,
    dynamic ab = _unspecified,
    dynamic ac = _unspecified,
    dynamic ad = _unspecified,
  ]) =>
      _step(
          'inject',
          _normalizeLiteralVarArgs([
            a,
            b,
            c,
            d,
            e,
            f,
            g,
            h,
            i,
            j,
            k,
            l,
            m,
            n,
            o,
            p,
            q,
            r,
            s,
            t,
            u,
            v,
            w,
            x,
            y,
            z,
            aa,
            ab,
            ac,
            ad
          ]));

  GraphTraversal to(dynamic toVertex) => _step('to', [toVertex]);

  GraphTraversal from_(dynamic fromVertex) => _step('from', [fromVertex]);

  GraphTraversal out(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified]) =>
      _step('out', _normalizeVarArgs([first, second, third]));

  GraphTraversal in_(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified]) =>
      _step('in', _normalizeVarArgs([first, second, third]));

  GraphTraversal both(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified]) =>
      _step('both', _normalizeVarArgs([first, second, third]));

  GraphTraversal outE(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified]) =>
      _step('outE', _normalizeVarArgs([first, second, third]));

  GraphTraversal inE(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified]) =>
      _step('inE', _normalizeVarArgs([first, second, third]));

  GraphTraversal bothE(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified]) =>
      _step('bothE', _normalizeVarArgs([first, second, third]));

  GraphTraversal outV() => _step('outV');

  GraphTraversal inV() => _step('inV');

  GraphTraversal bothV() => _step('bothV');

  GraphTraversal otherV() => _step('otherV');

  GraphTraversal order([dynamic scope]) =>
      _step('order', scope != null ? [scope] : null);

  GraphTraversal properties(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified]) =>
      _step('properties', _normalizeVarArgs([first, second, third]));

  GraphTraversal values(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified]) =>
      _step('values', _normalizeVarArgs([first, second, third]));

  GraphTraversal propertyMap(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified,
          dynamic fourth = _unspecified]) =>
      _step('propertyMap', _normalizeVarArgs([first, second, third, fourth]));

  GraphTraversal elementMap(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified,
          dynamic fourth = _unspecified]) =>
      _step('elementMap', _normalizeVarArgs([first, second, third, fourth]));

  GraphTraversal valueMap(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified,
          dynamic fourth = _unspecified]) =>
      _step('valueMap', _normalizeVarArgs([first, second, third, fourth]));

  GraphTraversal select(dynamic first,
          [dynamic second, dynamic third, dynamic fourth]) =>
      _step('select', [
        first,
        if (second != null) second,
        if (third != null) third,
        if (fourth != null) fourth
      ]);

  GraphTraversal by([dynamic arg, dynamic order]) =>
      _step('by', arg == null ? null : [arg, if (order != null) order]);

  GraphTraversal fold([dynamic seed, dynamic foldFunction]) {
    if (seed != null && foldFunction != null) {
      return _step('fold', [seed, foldFunction]);
    }
    return _step('fold');
  }

  GraphTraversal unfold() => _step('unfold');

  GraphTraversal path() => _step('path');

  GraphTraversal limit(dynamic scopeOrCount, [dynamic count]) {
    if (count != null) return _step('limit', [scopeOrCount, count]);
    return _step('limit', [scopeOrCount]);
  }

  GraphTraversal tail([dynamic scopeOrCount, dynamic count]) {
    if (count != null) return _step('tail', [scopeOrCount, count]);
    if (scopeOrCount != null) return _step('tail', [scopeOrCount]);
    return _step('tail');
  }

  GraphTraversal range(dynamic start, dynamic end, [dynamic count]) =>
      count == null
          ? _step('range', [start, end])
          : _step('range', [start, end, count]);

  GraphTraversal skip(dynamic scopeOrCount, [dynamic count]) {
    if (count != null) return _step('skip', [scopeOrCount, count]);
    return _step('skip', [scopeOrCount]);
  }

  GraphTraversal sample(dynamic scopeOrAmount, [dynamic amount]) {
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

  GraphTraversal filter_(dynamic traversalOrPredicate) =>
      filter(traversalOrPredicate);

  GraphTraversal has(dynamic first,
          [dynamic second = _unspecified, dynamic third = _unspecified]) =>
      _step('has', _normalizeVarArgs([first, second, third]));

  GraphTraversal hasLabel(dynamic first,
          [dynamic second = _unspecified, dynamic third = _unspecified]) =>
      _step('hasLabel', _normalizeVarArgs([first, second, third]));

  GraphTraversal hasId(dynamic first,
          [dynamic second = _unspecified, dynamic third = _unspecified]) =>
      _step('hasId', _normalizeVarArgs([first, second, third]));

  GraphTraversal hasKey(dynamic first,
          [dynamic second = _unspecified, dynamic third = _unspecified]) =>
      _step('hasKey', _normalizeVarArgs([first, second, third]));

  GraphTraversal hasValue(dynamic first,
          [dynamic second = _unspecified, dynamic third = _unspecified]) =>
      _step('hasValue', _normalizeVarArgs([first, second, third]));

  GraphTraversal hasNot(String key) => _step('hasNot', [key]);

  GraphTraversal and_(
          [dynamic first, dynamic second, dynamic third, dynamic fourth]) =>
      _step(
          'and',
          first == null
              ? null
              : _toTraversalList(first, second, third, fourth));

  GraphTraversal or_(
          [dynamic first, dynamic second, dynamic third, dynamic fourth]) =>
      _step(
          'or',
          first == null
              ? null
              : _toTraversalList(first, second, third, fourth));

  GraphTraversal not_(dynamic traversal) => _step('not', [traversal]);

  GraphTraversal where(dynamic predicateOrTraversal, [dynamic predicate]) =>
      _step('where', [predicateOrTraversal, if (predicate != null) predicate]);

  GraphTraversal is_(dynamic predicateOrValue) =>
      _step('is', [predicateOrValue]);

  GraphTraversal dedup(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified]) =>
      _step('dedup', _normalizeVarArgs([first, second, third]));

  GraphTraversal simplePath() => _step('simplePath');

  GraphTraversal cyclicPath() => _step('cyclicPath');

  // ---- Side-effect steps ---------------------------------------------------

  GraphTraversal sideEffect(dynamic traversalOrLambda) =>
      _step('sideEffect', [traversalOrLambda]);

  GraphTraversal call([dynamic procedure, dynamic traversal]) => _step('call',
      procedure == null ? null : [procedure, if (traversal != null) traversal]);

  GraphTraversal store(String key) => _step('store', [key]);

  GraphTraversal aggregate(dynamic first, [String? key]) =>
      _step('aggregate', [first, if (key != null) key]);

  GraphTraversal subgraph(String key) => _step('subgraph', [key]);

  GraphTraversal cap(dynamic first,
          [dynamic second = _unspecified, dynamic third = _unspecified]) =>
      _step('cap', _normalizeVarArgs([first, second, third]));

  GraphTraversal timeLimit(dynamic millis) => _step('timeLimit', [millis]);

  GraphTraversal profile([String? key]) =>
      _step('profile', key != null ? [key] : null);

  GraphTraversal property(dynamic first,
          [dynamic second = _unspecified,
          dynamic third = _unspecified,
          dynamic fourth = _unspecified,
          dynamic fifth = _unspecified,
          dynamic sixth = _unspecified,
          dynamic seventh = _unspecified,
          dynamic eighth = _unspecified]) =>
      _step(
          'property',
          _normalizeVarArgs(
              [first, second, third, fourth, fifth, sixth, seventh, eighth]));

  // ---- Branch steps --------------------------------------------------------

  GraphTraversal branch(dynamic traversal) => _step('branch', [traversal]);

  GraphTraversal choose(dynamic first, [dynamic second, dynamic third]) {
    final args = [first, if (second != null) second, if (third != null) third];
    return _step('choose', args);
  }

  GraphTraversal optional(dynamic traversal) => _step('optional', [traversal]);

  GraphTraversal union(
          [dynamic first, dynamic second, dynamic third, dynamic fourth]) =>
      _step(
          'union',
          first == null
              ? null
              : _toTraversalList(first, second, third, fourth));

  GraphTraversal coalesce(
          [dynamic first, dynamic second, dynamic third, dynamic fourth]) =>
      _step(
          'coalesce',
          first == null
              ? null
              : _toTraversalList(first, second, third, fourth));

  GraphTraversal repeat(dynamic traversalOrName, [dynamic traversal]) =>
      traversal == null
          ? _step('repeat', [traversalOrName])
          : _step('repeat', [traversalOrName, traversal]);

  GraphTraversal emit([dynamic traversalOrPredicate]) => _step(
      'emit', traversalOrPredicate != null ? [traversalOrPredicate] : null);

  GraphTraversal until(dynamic traversalOrPredicate) =>
      _step('until', [traversalOrPredicate]);

  GraphTraversal times(dynamic count) => _step('times', [count]);

  GraphTraversal local(dynamic traversal) => _step('local', [traversal]);

  // ---- Mutation steps ------------------------------------------------------

  GraphTraversal addV_([dynamic label]) =>
      _step('addV', label != null ? [label] : null);

  GraphTraversal addV([dynamic label]) => addV_(label);

  GraphTraversal addE_(dynamic label) => _step('addE', [label]);

  GraphTraversal addE(dynamic label) => addE_(label);

  GraphTraversal drop() => _step('drop');

  GraphTraversal mergeV_([dynamic args = _unspecified]) =>
      _step('mergeV', args != _unspecified ? [args] : null);

  GraphTraversal mergeV([dynamic args = _unspecified]) => mergeV_(args);

  GraphTraversal mergeE_([dynamic args = _unspecified]) =>
      _step('mergeE', args != _unspecified ? [args] : null);

  GraphTraversal mergeE([dynamic args = _unspecified]) => mergeE_(args);

  GraphTraversal option(dynamic first,
          [dynamic second = _unspecified, dynamic third = _unspecified]) =>
      _step('option', _normalizeVarArgs([first, second, third]));

  // ---- Math / string steps -------------------------------------------------

  GraphTraversal math_(String expression) => _step('math', [expression]);

  GraphTraversal concat_(List<dynamic> args) => _step('concat', args);

  GraphTraversal concat(
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified]) =>
      _step('concat', _normalizeVarArgs([first, second, third]));

  GraphTraversal toLower([dynamic scope]) =>
      _step('toLower', scope == null ? null : [scope]);

  GraphTraversal toUpper([dynamic scope]) =>
      _step('toUpper', scope == null ? null : [scope]);

  GraphTraversal trim([dynamic scope]) =>
      _step('trim', scope == null ? null : [scope]);

  GraphTraversal lTrim([dynamic scope]) =>
      _step('lTrim', scope == null ? null : [scope]);

  GraphTraversal rTrim([dynamic scope]) =>
      _step('rTrim', scope == null ? null : [scope]);

  GraphTraversal length_([dynamic scope]) =>
      _step('length', scope == null ? null : [scope]);

  GraphTraversal length([dynamic scope]) => length_(scope);

  GraphTraversal reverse() => _step('reverse');

  GraphTraversal replace(dynamic from, dynamic to, [dynamic replacement]) =>
      replacement == null
          ? _step('replace', [from, to])
          : _step('replace', [from, to, replacement]);

  GraphTraversal split(dynamic separator, [dynamic delimiter]) =>
      delimiter == null
          ? _step('split', [separator])
          : _step('split', [separator, delimiter]);

  GraphTraversal substring(dynamic start, [dynamic end, dynamic limit]) =>
      _step(
          'substring', [start, if (end != null) end, if (limit != null) limit]);

  GraphTraversal asString([dynamic scope]) =>
      _step('asString', scope == null ? null : [scope]);

  // ---- Misc steps ----------------------------------------------------------

  GraphTraversal as_(dynamic labels,
          [dynamic second = _unspecified, dynamic third = _unspecified]) =>
      _step('as', _normalizeVarArgs([labels, second, third]));

  GraphTraversal barrier([dynamic maxBarrierSize]) =>
      _step('barrier', maxBarrierSize != null ? [maxBarrierSize] : null);

  GraphTraversal index() => _step('index');

  GraphTraversal none_() => _step('none');

  GraphTraversal none(dynamic traversalOrPredicate) =>
      _step('none', [traversalOrPredicate]);

  GraphTraversal read() => _step('read');

  GraphTraversal write() => _step('write');

  GraphTraversal with_(String key, [dynamic value = _unspecified]) =>
      _step('with', _normalizeVarArgs([key, value]));

  GraphTraversal coin(dynamic probability) => _step('coin', [probability]);

  GraphTraversal element() => _step('element');

  GraphTraversal discard() => _step('discard');

  GraphTraversal fail([String? message]) =>
      _step('fail', message != null ? [message] : null);

  GraphTraversal intersect_(dynamic first, [dynamic second = _unspecified]) =>
      _step('intersect', _normalizeLiteralVarArgs([first, second]));

  GraphTraversal intersect(dynamic first, [dynamic second = _unspecified]) =>
      intersect_(first, second);

  GraphTraversal any_(dynamic traversalOrPredicate) =>
      _step('any', [traversalOrPredicate]);

  GraphTraversal all_(dynamic traversalOrPredicate) =>
      _step('all', [traversalOrPredicate]);

  GraphTraversal any(dynamic traversalOrPredicate) =>
      any_(traversalOrPredicate);

  GraphTraversal all(dynamic traversalOrPredicate) =>
      all_(traversalOrPredicate);

  // ignore: non_constant_identifier_names
  GraphTraversal none__(dynamic traversalOrPredicate) =>
      _step('none', [traversalOrPredicate]);

  GraphTraversal difference(dynamic first, [dynamic second = _unspecified]) =>
      _step('difference', _normalizeLiteralVarArgs([first, second]));

  GraphTraversal product(dynamic first, [dynamic second = _unspecified]) =>
      _step('product', _normalizeLiteralVarArgs([first, second]));

  GraphTraversal combine(dynamic first, [dynamic second = _unspecified]) =>
      _step('combine', _normalizeLiteralVarArgs([first, second]));

  GraphTraversal merge_(dynamic first, [dynamic second = _unspecified]) =>
      _step('merge', _normalizeLiteralVarArgs([first, second]));

  // ---- Additional steps ----

  GraphTraversal sack([dynamic operatorOrTraversal]) =>
      _step('sack', operatorOrTraversal != null ? [operatorOrTraversal] : null);

  GraphTraversal loops([String? variable]) =>
      _step('loops', variable != null ? [variable] : null);

  GraphTraversal match_(dynamic first,
          [dynamic second,
          dynamic third,
          dynamic fourth,
          dynamic fifth,
          dynamic sixth]) =>
      _step('match',
          _toTraversalList(first, second, third, fourth, fifth, sixth));

  GraphTraversal project(dynamic key,
          [dynamic second = _unspecified,
          dynamic third = _unspecified,
          dynamic fourth = _unspecified]) =>
      _step('project', _normalizeVarArgs([key, second, third, fourth]));

  GraphTraversal conjoin(String delimiter) => _step('conjoin', [delimiter]);

  /// Computes the set-theoretic disjunction (symmetric difference) of the
  /// incoming list/set traverser and [values] (a literal list or a traversal).
  GraphTraversal disjunct(dynamic values) => _step('disjunct', [values]);

  GraphTraversal format_(String template) => _step('format', [template]);

  GraphTraversal value_() => _step('value');

  GraphTraversal key_() => _step('key');

  GraphTraversal toV(dynamic direction) => _step('toV', [direction]);

  GraphTraversal toE(dynamic direction,
          [dynamic first = _unspecified,
          dynamic second = _unspecified,
          dynamic third = _unspecified]) =>
      _step('toE', _normalizeVarArgs([direction, first, second, third]));

  GraphTraversal asBool() => _step('asBool');

  GraphTraversal asDate() => _step('asDate');

  GraphTraversal asNumber([dynamic type]) =>
      _step('asNumber', type != null ? [type] : null);

  GraphTraversal dateAdd(dynamic chronoUnit, dynamic amount) =>
      _step('dateAdd', [chronoUnit, amount]);

  GraphTraversal dateDiff(dynamic other, [dynamic chronoUnit]) =>
      _step('dateDiff', [other, if (chronoUnit != null) chronoUnit]);

  GraphTraversal clone_() => _step('clone');

  // ---- OLAP steps ----

  GraphTraversal pageRank([dynamic alpha]) =>
      _step('pageRank', alpha != null ? [alpha] : null);

  GraphTraversal peerPressure() => _step('peerPressure');

  GraphTraversal connectedComponent() => _step('connectedComponent');

  GraphTraversal shortestPath() => _step('shortestPath');
}
