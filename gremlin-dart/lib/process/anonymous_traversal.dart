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
import 'graph_traversal.dart';
import 'traversal_strategy.dart';

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
    );
  }

  // Alias kept for API symmetry with other language drivers.
  GraphTraversalSource with_(dynamic connection) => withRemote(connection);
}

// Top-level factory function matching other drivers' `traversal()` call.
AnonymousTraversalSource traversal() => AnonymousTraversalSource.traversal();

// ---------------------------------------------------------------------------
// Anonymous traversal spawner — mirrors JS `__` object.
// Used inside child traversals (e.g. repeat(__.out('knows'))).
// ---------------------------------------------------------------------------

class Anon {
  Anon._(/* prevent instantiation */);

  static GraphTraversal _startAnon(String step, [List<dynamic>? args]) {
    final gl = GremlinLang()..addStep(step, args);
    return GraphTraversal(null, null, gl);
  }

  static GraphTraversal V([List<dynamic>? args]) => _startAnon('V', args);
  static GraphTraversal E([List<dynamic>? args]) => _startAnon('E', args);
  static GraphTraversal out([List<String>? labels]) => _startAnon('out', labels);
  static GraphTraversal in_([List<String>? labels]) => _startAnon('in', labels);
  static GraphTraversal both([List<String>? labels]) => _startAnon('both', labels);
  static GraphTraversal outE([List<String>? labels]) => _startAnon('outE', labels);
  static GraphTraversal inE([List<String>? labels]) => _startAnon('inE', labels);
  static GraphTraversal bothE([List<String>? labels]) => _startAnon('bothE', labels);
  static GraphTraversal outV() => _startAnon('outV');
  static GraphTraversal inV() => _startAnon('inV');
  static GraphTraversal bothV() => _startAnon('bothV');
  static GraphTraversal values([List<String>? keys]) => _startAnon('values', keys);
  static GraphTraversal properties([List<String>? keys]) => _startAnon('properties', keys);
  static GraphTraversal id() => _startAnon('id');
  static GraphTraversal label() => _startAnon('label');
  static GraphTraversal identity() => _startAnon('identity');
  static GraphTraversal count([dynamic scope]) =>
      _startAnon('count', scope != null ? [scope] : null);
  static GraphTraversal sum([dynamic scope]) =>
      _startAnon('sum', scope != null ? [scope] : null);
  static GraphTraversal max([dynamic scope]) =>
      _startAnon('max', scope != null ? [scope] : null);
  static GraphTraversal min([dynamic scope]) =>
      _startAnon('min', scope != null ? [scope] : null);
  static GraphTraversal mean([dynamic scope]) =>
      _startAnon('mean', scope != null ? [scope] : null);
  static GraphTraversal has(dynamic first, [dynamic second, dynamic third]) {
    final args = [first, if (second != null) second, if (third != null) third];
    return _startAnon('has', args);
  }
  static GraphTraversal hasLabel(dynamic label, [List<String>? rest]) =>
      _startAnon('hasLabel', [label, ...?rest]);
  static GraphTraversal hasId(dynamic id, [List<dynamic>? rest]) =>
      _startAnon('hasId', [id, ...?rest]);
  static GraphTraversal not_(dynamic traversal) => _startAnon('not', [traversal]);
  static GraphTraversal where(dynamic predicateOrTraversal) =>
      _startAnon('where', [predicateOrTraversal]);
  static GraphTraversal is_(dynamic value) => _startAnon('is', [value]);
  static GraphTraversal loops([String? loopName]) =>
      _startAnon('loops', loopName != null ? [loopName] : null);
  static GraphTraversal path() => _startAnon('path');
  static GraphTraversal select(dynamic key, [dynamic second]) =>
      _startAnon('select', [key, if (second != null) second]);
  static GraphTraversal fold() => _startAnon('fold');
  static GraphTraversal unfold() => _startAnon('unfold');
  static GraphTraversal limit(dynamic count) => _startAnon('limit', [count]);
  static GraphTraversal tail([dynamic count]) =>
      _startAnon('tail', count != null ? [count] : null);
  static GraphTraversal constant(dynamic value) => _startAnon('constant', [value]);
  static GraphTraversal union(List<dynamic> traversals) =>
      _startAnon('union', traversals);
  static GraphTraversal coalesce(List<dynamic> traversals) =>
      _startAnon('coalesce', traversals);
  static GraphTraversal repeat(dynamic traversal) =>
      _startAnon('repeat', [traversal]);
  static GraphTraversal emit([dynamic traversalOrPredicate]) =>
      _startAnon('emit', traversalOrPredicate != null ? [traversalOrPredicate] : null);
  static GraphTraversal until(dynamic traversalOrPredicate) =>
      _startAnon('until', [traversalOrPredicate]);
  static GraphTraversal times(int count) => _startAnon('times', [count]);
  static GraphTraversal local(dynamic traversal) =>
      _startAnon('local', [traversal]);
  static GraphTraversal math_(String expression) =>
      _startAnon('math', [expression]);
  static GraphTraversal addV_([dynamic label]) =>
      _startAnon('addV', label != null ? [label] : null);
  static GraphTraversal addE_(dynamic label) => _startAnon('addE', [label]);
  static GraphTraversal drop() => _startAnon('drop');
  static GraphTraversal optional(dynamic traversal) =>
      _startAnon('optional', [traversal]);
  static GraphTraversal dedup([List<dynamic>? args]) => _startAnon('dedup', args);
  static GraphTraversal inject(List<dynamic> args) => _startAnon('inject', args);
}
