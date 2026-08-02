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
import 'traversal_strategy.dart';

class Traversal {
  final Graph? graph;
  final TraversalStrategies? traversalStrategies;
  final GremlinLang gremlinLang;

  // Non-private so subclasses and strategies in other files can set them.
  Stream<dynamic>? resultsStream;
  Future<void>? _strategiesApplied;

  Traversal(this.graph, this.traversalStrategies,
      [GremlinLang? gremlinLang])
      : gremlinLang = gremlinLang ?? GremlinLang();

  GremlinLang getGremlinLang() => gremlinLang;

  Future<void> applyStrategies() {
    _strategiesApplied ??= traversalStrategies?.applyStrategies(this) ??
        Future.value();
    return _strategiesApplied!;
  }

  Future<List<T>> toList<T>() async {
    await applyStrategies();
    final result = <T>[];
    await for (final item in resultsStream ?? const Stream.empty()) {
      if (item is Traverser) {
        for (int i = 0; i < item.bulk; i++) {
          result.add(item.object as T);
        }
      } else {
        result.add(item as T);
      }
    }
    return result;
  }

  Future<T?> next<T>() async {
    await applyStrategies();
    if (resultsStream == null) return null;
    // For one-off next(), collect into list then take first; streaming
    // callers should use toList() or iterate() for efficiency.
    final items = await toList<T>();
    return items.isEmpty ? null : items.first;
  }

  Future<void> iterate() async {
    gremlinLang.addStep('discard');
    await applyStrategies();
    await resultsStream?.drain<void>();
  }

  @override
  String toString() => gremlinLang.getGremlin();
}

class RemoteTraversal extends Traversal {
  RemoteTraversal(Stream<dynamic> stream) : super(null, null) {
    resultsStream = stream;
  }
}

class Traverser<T> {
  final T object;
  int bulk;

  Traverser(this.object, [int? bulk]) : bulk = bulk ?? 1;
}

// ---------------------------------------------------------------------------
// Predicate
// ---------------------------------------------------------------------------

class P {
  final String operator;
  final dynamic value;
  final dynamic other;

  const P(this.operator, this.value, this.other);

  P and_(P arg) => P('and', this, arg);
  P or_(P arg) => P('or', this, arg);

  static P within(List<dynamic> args) => P('within', args, null);
  static P without(List<dynamic> args) => P('without', args, null);
  static P between(dynamic a, dynamic b) => P('between', a, b);
  static P eq(dynamic a) => P('eq', a, null);
  static P neq(dynamic a) => P('neq', a, null);
  static P gt(dynamic a) => P('gt', a, null);
  static P gte(dynamic a) => P('gte', a, null);
  static P lt(dynamic a) => P('lt', a, null);
  static P lte(dynamic a) => P('lte', a, null);
  static P inside(dynamic a, dynamic b) => P('inside', a, b);
  static P outside(dynamic a, dynamic b) => P('outside', a, b);
  static P not_(dynamic a) => P('not', a, null);
  static P test(dynamic a) => P('test', a, null);

  @override
  String toString() {
    if (other == null) return '$operator($value)';
    return '$operator($value, $other)';
  }
}

class TextP {
  final String operator;
  final dynamic value;
  final dynamic other;

  const TextP(this.operator, this.value, [this.other]);

  P and_(P arg) => P('and', this, arg);
  P or_(P arg) => P('or', this, arg);

  static TextP containing(String a) => TextP('containing', a);
  static TextP notContaining(String a) => TextP('notContaining', a);
  static TextP startingWith(String a) => TextP('startingWith', a);
  static TextP notStartingWith(String a) => TextP('notStartingWith', a);
  static TextP endingWith(String a) => TextP('endingWith', a);
  static TextP notEndingWith(String a) => TextP('notEndingWith', a);
  static TextP regex(String a) => TextP('regex', a);
  static TextP notRegex(String a) => TextP('notRegex', a);

  @override
  String toString() {
    if (other == null) return "$operator('$value')";
    return "$operator('$value', '$other')";
  }
}

// ---------------------------------------------------------------------------
// EnumValue
// ---------------------------------------------------------------------------

class EnumValue {
  final String typeName;
  final String elementName;

  const EnumValue(this.typeName, this.elementName);

  @override
  String toString() => '$typeName.$elementName';
}

// ---------------------------------------------------------------------------
// Numeric wrappers (match JS driver typed values)
// ---------------------------------------------------------------------------

class GInt {
  final int value;
  const GInt(this.value);
}

class GLong {
  final int value;
  const GLong(this.value);
}

class GFloat {
  final double value;
  const GFloat(this.value);
}

class GDouble {
  final double value;
  const GDouble(this.value);
}

class GShort {
  final int value;
  const GShort(this.value);
}

class GByte {
  final int value;
  const GByte(this.value);
}

class GDecimal {
  final int scale;
  final BigInt unscaled;
  const GDecimal(this.scale, this.unscaled);

  double toDouble() => unscaled.toDouble() / _pow10(scale);

  static double _pow10(int n) {
    double r = 1.0;
    for (var i = 0; i < n; i++) r *= 10.0;
    return r;
  }

  @override
  String toString() => 'GDecimal($unscaled e-$scale)';
}

// ---------------------------------------------------------------------------
// Enum singletons (mirrors JS driver exports)
// ---------------------------------------------------------------------------

EnumValue _e(String type, String name) => EnumValue(type, name);

final barrier = (normSack: _e('Barrier', 'normSack'),);
final cardinality = (
  list: _e('Cardinality', 'list'),
  set_: _e('Cardinality', 'set'),
  single: _e('Cardinality', 'single'),
);
final column = (
  keys: _e('Column', 'keys'),
  values: _e('Column', 'values'),
);
final direction = (
  both: _e('Direction', 'BOTH'),
  in_: _e('Direction', 'IN'),
  out: _e('Direction', 'OUT'),
);
final dt = (
  second: _e('DT', 'second'),
  minute: _e('DT', 'minute'),
  hour: _e('DT', 'hour'),
  day: _e('DT', 'day'),
);
final merge = (
  onCreate: _e('Merge', 'onCreate'),
  onMatch: _e('Merge', 'onMatch'),
  outV: _e('Merge', 'outV'),
  inV: _e('Merge', 'inV'),
);
final operator_ = (
  addAll: _e('Operator', 'addAll'),
  and_: _e('Operator', 'and'),
  assign: _e('Operator', 'assign'),
  div: _e('Operator', 'div'),
  max: _e('Operator', 'max'),
  min: _e('Operator', 'min'),
  minus: _e('Operator', 'minus'),
  mult: _e('Operator', 'mult'),
  or_: _e('Operator', 'or'),
  sum: _e('Operator', 'sum'),
  sumLong: _e('Operator', 'sumLong'),
);
final order = (
  asc: _e('Order', 'asc'),
  desc: _e('Order', 'desc'),
  shuffle: _e('Order', 'shuffle'),
);
final pop = (
  all: _e('Pop', 'all'),
  first: _e('Pop', 'first'),
  last: _e('Pop', 'last'),
  mixed: _e('Pop', 'mixed'),
);
final scope = (
  global: _e('Scope', 'global'),
  local: _e('Scope', 'local'),
);
final t = (
  id: _e('T', 'id'),
  key: _e('T', 'key'),
  label: _e('T', 'label'),
  value: _e('T', 'value'),
);
final pick = (
  any: _e('Pick', 'any'),
  none: _e('Pick', 'none'),
);

const withOptions = (
  tokens: '~tinkerpop.valueMap.tokens',
  none: 0,
  ids: 1,
  labels: 2,
  keys: 4,
  values: 8,
  all: 15,
  indexer: '~tinkerpop.index.indexer',
  list: 0,
  map: 1,
);
