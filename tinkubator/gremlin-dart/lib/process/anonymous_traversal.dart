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

import '../driver/driver_remote_connection.dart';
import '../structure/graph.dart';
import 'gremlin_lang.dart';
import 'graph_traversal.dart';
import 'traversal_strategy.dart';

const Object _anonUnspecified = Object();

class AnonymousTraversalSource {
  const AnonymousTraversalSource._();

  static AnonymousTraversalSource traversal() =>
      const AnonymousTraversalSource._();

  GraphTraversalSource withRemote(dynamic connection) {
    final strategies = TraversalStrategies();
    strategies.addStrategy(RemoteStrategy(connection));
    return GraphTraversalSource(
      Graph(),
      strategies,
      GremlinLang(),
      connection is TransactionCapableRemoteConnectionBase ? connection : null,
      connection is DriverRemoteConnection
          ? connection.options.traversalSource
          : null,
    );
  }

  // Alias kept for API symmetry with other language drivers.
  GraphTraversalSource with_(dynamic connection) => withRemote(connection);
}

// Top-level factory function matching other drivers' `traversal()` call.
AnonymousTraversalSource traversal() => AnonymousTraversalSource.traversal();

// ---------------------------------------------------------------------------
// __ — anonymous traversal spawner.
// Equivalent to Java's __ class and Python's __ module.
// Method names mirror DartTranslateVisitor's processGremlinSymbol output so
// that generated gremlin.dart code compiles without changes.
// ---------------------------------------------------------------------------

class Anon {
  Anon._();

  static GraphTraversal _a() => GraphTraversal(null, null, GremlinLang());

  // Source-spawn steps (create anonymous traversals rooted at a step)
  static GraphTraversal V(
          [dynamic first = _anonUnspecified,
          dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified]) =>
      GraphTraversal(
          null,
          null,
          GremlinLang()
            ..addStep('V', _normalizeAnonVarArgs([first, second, third])));

  static GraphTraversal E(
          [dynamic first = _anonUnspecified,
          dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified]) =>
      GraphTraversal(
          null,
          null,
          GremlinLang()
            ..addStep('E', _normalizeAnonVarArgs([first, second, third])));

  static GraphTraversal inject([
    dynamic a = _anonUnspecified,
    dynamic b = _anonUnspecified,
    dynamic c = _anonUnspecified,
    dynamic d = _anonUnspecified,
    dynamic e = _anonUnspecified,
    dynamic f = _anonUnspecified,
    dynamic g = _anonUnspecified,
    dynamic h = _anonUnspecified,
    dynamic i = _anonUnspecified,
    dynamic j = _anonUnspecified,
    dynamic k = _anonUnspecified,
    dynamic l = _anonUnspecified,
    dynamic m = _anonUnspecified,
    dynamic n = _anonUnspecified,
    dynamic o = _anonUnspecified,
    dynamic p = _anonUnspecified,
    dynamic q = _anonUnspecified,
    dynamic r = _anonUnspecified,
    dynamic s = _anonUnspecified,
    dynamic t = _anonUnspecified,
    dynamic u = _anonUnspecified,
    dynamic v = _anonUnspecified,
    dynamic w = _anonUnspecified,
    dynamic x = _anonUnspecified,
    dynamic y = _anonUnspecified,
    dynamic z = _anonUnspecified,
    dynamic aa = _anonUnspecified,
    dynamic ab = _anonUnspecified,
    dynamic ac = _anonUnspecified,
    dynamic ad = _anonUnspecified,
  ]) =>
      GraphTraversal(
          null,
          null,
          GremlinLang()
            ..addStep(
                'inject',
                _normalizeAnonLiteralVarArgs([
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

  static GraphTraversal addV([dynamic label]) => _a().addV_(label);
  static GraphTraversal addE(dynamic label) => _a().addE(label);

  // Vertex/edge traversal steps
  static GraphTraversal out(
          [dynamic first = _anonUnspecified,
          dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified]) =>
      _a().out(first, second, third);
  static GraphTraversal in_(
          [dynamic first = _anonUnspecified,
          dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified]) =>
      _a().in_(first, second, third);
  static GraphTraversal both(
          [dynamic first = _anonUnspecified,
          dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified]) =>
      _a().both(first, second, third);
  static GraphTraversal outE(
          [dynamic first = _anonUnspecified,
          dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified]) =>
      _a().outE(first, second, third);
  static GraphTraversal inE(
          [dynamic first = _anonUnspecified,
          dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified]) =>
      _a().inE(first, second, third);
  static GraphTraversal bothE(
          [dynamic first = _anonUnspecified,
          dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified]) =>
      _a().bothE(first, second, third);
  static GraphTraversal outV() => _a().outV();
  static GraphTraversal inV() => _a().inV();
  static GraphTraversal bothV() => _a().bothV();
  static GraphTraversal otherV() => _a().otherV();

  // Property steps
  static GraphTraversal values(
          [dynamic first = _anonUnspecified,
          dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified]) =>
      _a().values(first, second, third);
  static GraphTraversal properties(
          [dynamic first = _anonUnspecified,
          dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified]) =>
      _a().properties(first, second, third);
  static GraphTraversal propertyMap([dynamic keys = _anonUnspecified]) =>
      identical(keys, _anonUnspecified)
          ? _a().propertyMap()
          : _a().propertyMap(keys);
  static GraphTraversal elementMap([dynamic keys = _anonUnspecified]) =>
      identical(keys, _anonUnspecified)
          ? _a().elementMap()
          : _a().elementMap(keys);
  static GraphTraversal valueMap([dynamic args = _anonUnspecified]) =>
      identical(args, _anonUnspecified) ? _a().valueMap() : _a().valueMap(args);
  static GraphTraversal property(dynamic first,
          [dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified,
          dynamic fourth = _anonUnspecified,
          dynamic fifth = _anonUnspecified,
          dynamic sixth = _anonUnspecified,
          dynamic seventh = _anonUnspecified,
          dynamic eighth = _anonUnspecified]) =>
      GraphTraversal(
          null,
          null,
          GremlinLang()
            ..addStep(
                'property',
                _normalizeAnonVarArgs([
                  first,
                  second,
                  third,
                  fourth,
                  fifth,
                  sixth,
                  seventh,
                  eighth
                ])));

  // Identity / navigation
  static GraphTraversal id() => _a().id();
  static GraphTraversal label() => _a().label();
  static GraphTraversal identity() => _a().identity();
  static GraphTraversal constant(dynamic value) => _a().constant(value);

  // Filter steps
  static GraphTraversal has(dynamic first,
          [dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified]) =>
      _a().has(first, second, third);
  static GraphTraversal hasLabel(dynamic first,
          [dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified]) =>
      _a().hasLabel(first, second, third);
  static GraphTraversal hasId(dynamic first,
          [dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified]) =>
      _a().hasId(first, second, third);
  static GraphTraversal hasNot(String key) => _a().hasNot(key);
  static GraphTraversal where(dynamic predicateOrTraversal,
          [dynamic predicate]) =>
      _a().where(predicateOrTraversal, predicate);
  static GraphTraversal is_(dynamic predicateOrValue) =>
      _a().is_(predicateOrValue);
  static GraphTraversal not_(dynamic traversal) => _a().not_(traversal);
  static GraphTraversal dedup(
          [dynamic first = _anonUnspecified,
          dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified]) =>
      _a().dedup(first, second, third);
  static GraphTraversal simplePath() => _a().simplePath();
  static GraphTraversal cyclicPath() => _a().cyclicPath();
  static GraphTraversal filter_(dynamic traversalOrPredicate) =>
      _a().filter(traversalOrPredicate);

  // Aggregation / reduction
  static GraphTraversal count([dynamic scope]) => _a().count(scope);
  static GraphTraversal sum([dynamic scope]) => _a().sum(scope);
  static GraphTraversal max([dynamic scope]) => _a().max(scope);
  static GraphTraversal min([dynamic scope]) => _a().min(scope);
  static GraphTraversal mean([dynamic scope]) => _a().mean(scope);
  static GraphTraversal fold([dynamic seed, dynamic foldFunction]) =>
      _a().fold(seed, foldFunction);
  static GraphTraversal unfold() => _a().unfold();

  // Map / select
  static GraphTraversal map_(dynamic traversalOrLambda) =>
      _a().map_(traversalOrLambda);
  static GraphTraversal flatMap(dynamic traversalOrLambda) =>
      _a().flatMap(traversalOrLambda);
  static GraphTraversal select(dynamic first,
          [dynamic second, dynamic third, dynamic fourth]) =>
      _a().select(first, second, third, fourth);
  static GraphTraversal project(dynamic key,
          [dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified,
          dynamic fourth = _anonUnspecified]) =>
      _a().project(key, second, third, fourth);
  static GraphTraversal by(dynamic arg, [dynamic order]) => _a().by(arg, order);

  // Logic
  static GraphTraversal and_(
          [dynamic first, dynamic second, dynamic third, dynamic fourth]) =>
      _a().and_(first, second, third, fourth);
  static GraphTraversal or_(
          [dynamic first, dynamic second, dynamic third, dynamic fourth]) =>
      _a().or_(first, second, third, fourth);

  // Ordering
  static GraphTraversal order([dynamic scope]) => _a().order(scope);

  // Path
  static GraphTraversal path() => _a().path();

  // Range / limit / tail
  static GraphTraversal limit(dynamic scopeOrCount, [dynamic count]) =>
      _a().limit(scopeOrCount, count);
  static GraphTraversal tail([dynamic scopeOrCount, dynamic count]) =>
      _a().tail(scopeOrCount, count);
  static GraphTraversal range(dynamic start, dynamic end, [dynamic count]) =>
      _a().range(start, end, count);
  static GraphTraversal skip(dynamic scopeOrCount, [dynamic count]) =>
      _a().skip(scopeOrCount, count);

  // Branch steps
  static GraphTraversal branch(dynamic traversal) => _a().branch(traversal);
  static GraphTraversal choose(dynamic first,
          [dynamic second, dynamic third]) =>
      _a().choose(first, second, third);
  static GraphTraversal optional(dynamic traversal) => _a().optional(traversal);
  static GraphTraversal union(dynamic first,
          [dynamic second, dynamic third, dynamic fourth]) =>
      _a().union(first, second, third, fourth);
  static GraphTraversal coalesce(dynamic first,
          [dynamic second, dynamic third, dynamic fourth]) =>
      _a().coalesce(first, second, third, fourth);
  static GraphTraversal repeat(dynamic traversal) => _a().repeat(traversal);
  static GraphTraversal emit([dynamic traversalOrPredicate]) =>
      _a().emit(traversalOrPredicate);
  static GraphTraversal until(dynamic traversalOrPredicate) =>
      _a().until(traversalOrPredicate);
  static GraphTraversal times(dynamic count) => _a().times(count);
  static GraphTraversal loops([String? loopName]) => _a().loops(loopName);
  static GraphTraversal local(dynamic traversal) => _a().local(traversal);

  // Matching
  static GraphTraversal match_(dynamic first,
          [dynamic second,
          dynamic third,
          dynamic fourth,
          dynamic fifth,
          dynamic sixth]) =>
      _a().match_(first, second, third, fourth, fifth, sixth);
  static GraphTraversal as_(dynamic labels,
          [dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified]) =>
      _a().as_(labels, second, third);

  // Group / aggregate
  static GraphTraversal group([dynamic sideEffectKey]) =>
      _a().group(sideEffectKey);
  static GraphTraversal groupCount([String? key]) => _a().groupCount(key);
  static GraphTraversal aggregate(dynamic first, [String? key]) =>
      _a().aggregate(first, key);
  static GraphTraversal cap(String first, [List<String>? rest]) =>
      _a().cap(first, rest);
  static GraphTraversal tree([String? key]) => _a().tree(key);
  static GraphTraversal sack([dynamic operatorOrTraversal]) =>
      _a().sack(operatorOrTraversal);
  static GraphTraversal sideEffect(dynamic traversalOrLambda) =>
      _a().sideEffect(traversalOrLambda);

  // Math & misc
  static GraphTraversal math_(String expression) => _a().math_(expression);
  static GraphTraversal concat(dynamic first,
          [dynamic second = _anonUnspecified,
          dynamic third = _anonUnspecified]) =>
      _a().concat(first, second, third);
  static GraphTraversal intersect(dynamic first,
          [dynamic second = _anonUnspecified]) =>
      identical(second, _anonUnspecified)
          ? _a().intersect(first)
          : _a().intersect(first, second);
  static GraphTraversal length([dynamic scope]) => _a().length(scope);
  static GraphTraversal sample(dynamic scopeOrAmount, [dynamic amount]) =>
      _a().sample(scopeOrAmount, amount);
  static GraphTraversal split(dynamic separator, [dynamic delimiter]) =>
      _a().split(separator, delimiter);
  static GraphTraversal dateAdd(dynamic chronoUnit, dynamic amount) =>
      _a().dateAdd(chronoUnit, amount);
  static GraphTraversal dateDiff(dynamic other, [dynamic chronoUnit]) =>
      _a().dateDiff(other, chronoUnit);

  // Service / call step
  static GraphTraversal call_(String procedure, [List<dynamic>? args]) =>
      GraphTraversal(
          null, null, GremlinLang()..addStep('call', [procedure, ...?args]));
  static GraphTraversal call([dynamic procedure, dynamic traversal]) =>
      _a().call(procedure, traversal);

  // Discard / fail
  static GraphTraversal discard() => _a().discard();
  static GraphTraversal fail([String? message]) => _a().fail(message);

  // Mutation steps
  static GraphTraversal drop() => _a().drop();
  static GraphTraversal addE_(dynamic label) => _a().addE_(label);

  // OLAP
  static GraphTraversal pageRank([dynamic alpha]) => _a().pageRank(alpha);
  static GraphTraversal peerPressure() => _a().peerPressure();
  static GraphTraversal connectedComponent() => _a().connectedComponent();
  static GraphTraversal shortestPath() => _a().shortestPath();
}

List<dynamic>? _normalizeAnonVarArgs(List<dynamic> args) {
  final values = List<dynamic>.from(args);
  while (values.isNotEmpty && identical(values.last, _anonUnspecified)) {
    values.removeLast();
  }
  if (values.isEmpty) return null;
  return values.length == 1 && values.first is List
      ? values.first as List<dynamic>
      : values;
}

List<dynamic>? _normalizeAnonLiteralVarArgs(List<dynamic> args) {
  final values = List<dynamic>.from(args);
  while (values.isNotEmpty && identical(values.last, _anonUnspecified)) {
    values.removeLast();
  }
  return values.isEmpty ? null : values;
}
