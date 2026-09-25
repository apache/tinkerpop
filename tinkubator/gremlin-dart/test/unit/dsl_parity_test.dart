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

import 'package:test/test.dart';
import 'package:uuid/uuid_value.dart';
import 'dart:io';

import '../../lib/process/anonymous_traversal.dart';
import '../../lib/process/graph_traversal.dart';
import '../../lib/process/traversal.dart';
import '../../lib/process/traversal_strategy.dart';
import '../../lib/structure/graph.dart';
import '../../lib/structure/io/graph_binary/graph_binary_reader.dart';
import '../../lib/structure/io/graph_binary/graph_binary_writer.dart';

void main() {
  GraphTraversalSource _g() =>
      GraphTraversalSource(Graph(), TraversalStrategies());

  // ---------------------------------------------------------------------------
  // GraphTraversalSource.withoutStrategies
  // ---------------------------------------------------------------------------
  group('GraphTraversalSource.withoutStrategies', () {
    test('produces withoutStrategies source step with unquoted class name', () {
      final gs = _g().withoutStrategies(['ConnectiveStrategy']);
      final gremlin = gs.gremlinLang.getGremlin();
      // Strategy names must be bare identifiers, not quoted strings.
      expect(gremlin, contains('withoutStrategies(ConnectiveStrategy)'));
      expect(gremlin, isNot(contains("'ConnectiveStrategy'")));
    });

    test('multiple strategy names are unquoted identifiers separated by commas',
        () {
      final gs =
          _g().withoutStrategies(['ReadOnlyStrategy', 'OptionsStrategy']);
      final gremlin = gs.gremlinLang.getGremlin();
      expect(gremlin,
          contains('withoutStrategies(ReadOnlyStrategy,OptionsStrategy)'));
      expect(gremlin, isNot(contains("'ReadOnlyStrategy'")));
      expect(gremlin, isNot(contains("'OptionsStrategy'")));
    });

    test('does not mutate original source', () {
      final base = _g();
      final gs1 = base.withoutStrategies(['ConnectiveStrategy']);
      final gs2 = base.withoutStrategies(['ReadOnlyStrategy']);
      expect(gs1.gremlinLang.getGremlin(),
          isNot(equals(gs2.gremlinLang.getGremlin())));
      expect(
          base.gremlinLang.getGremlin(), isNot(contains('withoutStrategies')));
    });

    test('can be chained with traversal steps', () {
      final t = _g().withoutStrategies(['ConnectiveStrategy']).V();
      expect(t.gremlinLang.getGremlin(), contains('V()'));
    });

    test('can be combined with withStrategies', () {
      final gs = _g().withStrategies([ReadOnlyStrategy()]).withoutStrategies(
          ['EarlyLimitStrategy']);
      final gremlin = gs.gremlinLang.getGremlin();
      expect(gremlin, contains('withStrategies'));
      expect(gremlin, contains('withoutStrategies(EarlyLimitStrategy)'));
    });

    test('empty list produces no withoutStrategies step', () {
      final gs = _g().withoutStrategies([]);
      final gremlin = gs.gremlinLang.getGremlin();
      expect(gremlin, isNot(contains('withoutStrategies')));
    });
  });

  // ---------------------------------------------------------------------------
  // Code-generation regressions
  // ---------------------------------------------------------------------------
  group('Generated traversal regressions', () {
    test('generator preserves every script in scenario order', () {
      final generator = File('build/generate.groovy').readAsStringSync();
      expect(generator, isNot(contains('scripts.last()')));
      expect(generator, contains('scripts.collect'));
      expect(generator, contains('translatedScripts.each'));

      final generated = File('test/feature/gremlin.dart').readAsStringSync();
      final scenario = RegExp(
        r"'data/Map\.feature::g_V_valuesXmapX_isXtypeOfXGType_MAPXX_countXlocalX':[\s\S]*?\n  \],",
      ).firstMatch(generated);
      expect(scenario, isNotNull);
      expect(scenario!.group(0), contains("g.addV('data').property('map'"));
      expect(scenario.group(0), contains("g.V().values('map')"));
    });

    test('generated null inject scenario preserves explicit null arguments',
        () {
      final generated = File('test/feature/gremlin.dart').readAsStringSync();
      expect(generated, contains('g_injectXnull_1_3_nullX'));
      expect(generated, contains('g.inject(null, GInt(1), GInt(3), null)'));
    });
  });

  // ---------------------------------------------------------------------------
  // Vararg/null preservation regressions
  // ---------------------------------------------------------------------------
  group('Traversal vararg regressions', () {
    test('an empty map argument serializes as an empty map', () {
      final gremlin = _g().mergeV(<String, dynamic>{}).gremlinLang.getGremlin();
      expect(gremlin, 'g.mergeV([:])');
      expect(gremlin, isNot(contains('null:null')));
    });

    test('withSack two-argument form is not serialized as a nested list', () {
      final gremlin =
          _g().withSack(GInt(1), operator_.sum).V().gremlinLang.getGremlin();
      expect(gremlin, 'g.withSack(1,Operator.sum).V()');
      expect(gremlin, isNot(contains('[1,Operator.sum]')));
    });

    test('source inject preserves explicit nulls between values', () {
      final gremlin = _g().inject(null, GInt(1), null).gremlinLang.getGremlin();
      expect(gremlin, 'g.inject(null,1,null)');
    });

    test('has preserves an explicit null value argument', () {
      expect(_g().V().has('name', null).gremlinLang.getGremlin(),
          "g.V().has('name',null)");
      expect(Anon.has('name').gremlinLang.getGremlin('__'), "__.has('name')");
      expect(Anon.has('name', null).gremlinLang.getGremlin('__'),
          "__.has('name',null)");
    });

    test('with preserves an explicit null value argument', () {
      expect(_g().V().with_('option').gremlinLang.getGremlin(),
          "g.V().with('option')");
      expect(_g().V().with_('option', null).gremlinLang.getGremlin(),
          "g.V().with('option',null)");

      final source = _g().with_('option', null);
      final options = source.gremlinLang.getOptionsStrategies();
      expect(options.single.configuration['option'], isNull);
    });

    test('collection steps preserve an explicit null second argument', () {
      expect(_g().V().combine(scope.local, null).gremlinLang.getGremlin(),
          'g.V().combine(Scope.local,null)');
      expect(_g().V().difference(scope.local, null).gremlinLang.getGremlin(),
          'g.V().difference(Scope.local,null)');
      expect(_g().V().intersect(scope.local, null).gremlinLang.getGremlin(),
          'g.V().intersect(Scope.local,null)');
      expect(_g().V().merge_(scope.local, null).gremlinLang.getGremlin(),
          'g.V().merge(Scope.local,null)');
      expect(_g().V().product(scope.local, null).gremlinLang.getGremlin(),
          'g.V().product(Scope.local,null)');
      expect(
        Anon.intersect(scope.local, null).gremlinLang.getGremlin('__'),
        '__.intersect(Scope.local,null)',
      );
    });

    test('collection steps preserve a single list argument', () {
      expect(
        _g().V().combine(['a', 'b']).gremlinLang.getGremlin(),
        "g.V().combine(['a','b'])",
      );
      expect(
        _g().V().difference(['a', 'b']).gremlinLang.getGremlin(),
        "g.V().difference(['a','b'])",
      );
    });

    test('map steps preserve explicit null varargs', () {
      expect(
        _g().V().elementMap('name', 'age', null).gremlinLang.getGremlin(),
        "g.V().elementMap('name','age',null)",
      );
      expect(
        _g().V().valueMap('name', 'age', null).gremlinLang.getGremlin(),
        "g.V().valueMap('name','age',null)",
      );
      expect(
        Anon.elementMap().gremlinLang.getGremlin('__'),
        '__.elementMap()',
      );
      expect(
        Anon.valueMap(null).gremlinLang.getGremlin('__'),
        '__.valueMap(null)',
      );
    });

    test('anonymous inject preserves explicit nulls between values', () {
      final gremlin =
          Anon.inject(null, GInt(1), null).gremlinLang.getGremlin('__');
      expect(gremlin, '__.inject(null,1,null)');
    });

    test('anonymous source and mutation steps serialize correctly', () {
      expect(Anon.E().gremlinLang.getGremlin('__'), '__.E()');
      expect(Anon.out().gremlinLang.getGremlin('__'), '__.out()');
      expect(Anon.properties().gremlinLang.getGremlin('__'), '__.properties()');
      expect(
        Anon.sideEffect(Anon.identity()).gremlinLang.getGremlin('__'),
        '__.sideEffect(__.identity())',
      );
      expect(
        Anon.property(cardinality.single, 'age', GInt(22))
            .gremlinLang
            .getGremlin('__'),
        "__.property(Cardinality.single,'age',22)",
      );
    });

    test('single list argument remains a single list argument', () {
      final gremlin = _g().inject([null, GInt(1)]).gremlinLang.getGremlin();
      expect(gremlin, 'g.inject([null,1])');
    });

    test('an empty list argument is kept, not spread into no arguments', () {
      expect(_g().V().hasId([]).gremlinLang.getGremlin(), 'g.V().hasId([])');
      // a non-empty lone list is still spread into varargs
      expect(
          _g().V().hasId([1, 2]).gremlinLang.getGremlin(), 'g.V().hasId(1,2)');
    });

    test('newly widened steps accept their full generated arity', () {
      expect(_g().V(1, 2, 3, 4).gremlinLang.getGremlin(), 'g.V(1,2,3,4)');
      expect(_g().V().to(direction.OUT, 'knows').gremlinLang.getGremlin(),
          "g.V().to(Direction.OUT,'knows')");
      expect(Anon.repeat('a', Anon.out()).gremlinLang.getGremlin('__'),
          "__.repeat('a',__.out())");
    });

    test('withStrategies and withoutStrategies accept varargs or a list', () {
      expect(_g().withStrategies(ReadOnlyStrategy()).gremlinLang.getGremlin(),
          'g.withStrategies(ReadOnlyStrategy)');
      expect(
          _g()
              .withStrategies(SubgraphStrategy(
                  vertices: Anon.has('name'), checkAdjacentVertices: false))
              .gremlinLang
              .getGremlin(),
          "g.withStrategies(new SubgraphStrategy(vertices:__.has('name'),checkAdjacentVertices:false))");
      // The list form remains supported alongside varargs.
      expect(_g().withStrategies([ReadOnlyStrategy()]).gremlinLang.getGremlin(),
          'g.withStrategies(ReadOnlyStrategy)');
      // a strategy class may be passed directly, as generated code does
      expect(
          _g().withoutStrategies(RepeatUnrollStrategy).gremlinLang.getGremlin(),
          'g.withoutStrategies(RepeatUnrollStrategy)');
      expect(
        _g().withoutStrategies(SeedStrategy).gremlinLang.getGremlin(),
        'g.withoutStrategies(SeedStrategy)',
      );
      expect(
        () => _g().withoutStrategies(DateTime).gremlinLang.getGremlin(),
        throwsArgumentError,
      );
    });

    test(
        'withoutStrategies ignores empty names rather than emitting a trailing comma',
        () {
      expect(_g().withoutStrategies('A', '').gremlinLang.getGremlin(),
          'g.withoutStrategies(A)');
      expect(_g().withoutStrategies('').gremlinLang.getGremlin(), 'g');
    });

    test('asPlainInt unwraps GInt/GLong for driver-side option values', () {
      // OptionsStrategy(evaluationTimeout: GInt(500)) used to throw a TypeError
      // when DriverRemoteConnection/Cluster cast the config value straight to
      // int; the same GInt literal a real user or generated scenario would use.
      expect(asPlainInt(GInt(500)), 500);
      expect(asPlainInt(GLong(500)), 500);
      expect(asPlainInt(500), 500);
      expect(asPlainInt(null), isNull);
    });

    test('strategyNameOf is stable and rejects unknown types', () {
      expect(strategyNameOf(ReadOnlyStrategy), 'ReadOnlyStrategy');
      expect(strategyNameOf(ReadOnlyStrategy()), 'ReadOnlyStrategy');
      expect(strategyNameOf('Custom'), 'Custom');
      expect(() => strategyNameOf(DateTime), throwsArgumentError);
    });

    test('generated feature traversal catalog does not regress', () {
      final generated = File('test/feature/gremlin.dart').readAsStringSync();
      final count = RegExp(r"^  '[^']+': <Function>\[", multiLine: true)
          .allMatches(generated)
          .length;
      expect(count, greaterThanOrEqualTo(2100));
    });

    test('generated code escapes dollar signs in string literals', () {
      final generated = File('test/feature/gremlin.dart').readAsStringSync();
      // an unescaped \$ would be Dart string interpolation
      expect(generated, contains(r'MatchStep\$CountMatchAlgorithm'));
    });

    test('merge source steps distinguish an omitted argument from null', () {
      expect(_g().mergeV().gremlinLang.getGremlin(), 'g.mergeV()');
      expect(_g().mergeV(null).gremlinLang.getGremlin(), 'g.mergeV(null)');
      expect(_g().mergeE().gremlinLang.getGremlin(), 'g.mergeE()');
      expect(_g().mergeE(null).gremlinLang.getGremlin(), 'g.mergeE(null)');
    });

    test('merge traversal steps preserve an explicit null argument', () {
      expect(
          _g().V().mergeV(null).gremlinLang.getGremlin(), 'g.V().mergeV(null)');
      expect(
          _g().V().mergeE(null).gremlinLang.getGremlin(), 'g.V().mergeE(null)');
    });

    test('UUID values serialize as Gremlin UUID literals', () {
      final uuid = UuidValue.fromString('f47af10b-58cc-4372-a567-0f02b2f3d479');
      expect(
        _g().inject(uuid).gremlinLang.getGremlin(),
        'g.inject(UUID("f47af10b-58cc-4372-a567-0f02b2f3d479"))',
      );
    });

    test('nested merge cardinality values are preserved', () {
      expect(
        _g()
            .mergeV({'name': 'marko'})
            .option(merge.onMatch, {'age': cardinality.list(GInt(33))})
            .gremlinLang
            .getGremlin(),
        "g.mergeV(['name':'marko']).option(Merge.onMatch,['age':Cardinality.list(33)])",
      );
    });

    test('enum keys in merge maps serialize as parenthesized keys', () {
      expect(
        _g()
            .mergeE({
              t.label: 'self',
              direction.OUT: merge.outV,
              direction.IN: merge.inV
            })
            .gremlinLang
            .getGremlin(),
        "g.mergeE([(T.label):'self',(Direction.OUT):Merge.outV,(Direction.IN):Merge.inV])",
      );
    });

    test('cardinality values serialize as Gremlin cardinality literals', () {
      expect(
        _g()
            .mergeV({'name': 'alice'})
            .option(merge.onCreate, {'age': cardinality.single(GInt(81))})
            .gremlinLang
            .getGremlin(),
        "g.mergeV(['name':'alice']).option(Merge.onCreate,['age':Cardinality.single(81)])",
      );
    });
  });

  // ---------------------------------------------------------------------------
  // GraphTraversal.disjunct
  // ---------------------------------------------------------------------------
  group('GraphTraversal.disjunct', () {
    test('produces disjunct step with list argument', () {
      final t = _g().V().fold().disjunct(['marko', 'vadas']);
      expect(t.gremlinLang.getGremlin(), contains('disjunct'));
    });

    test('produces disjunct step with anonymous traversal argument', () {
      final t = _g().V().fold().disjunct(Anon.V().fold());
      expect(t.gremlinLang.getGremlin(), contains('disjunct'));
    });

    test('disjunct is chainable', () {
      final t = _g().V().fold().disjunct(['x']).count(scope.local);
      final gremlin = t.gremlinLang.getGremlin();
      expect(gremlin, contains('disjunct'));
      expect(gremlin, contains('count'));
    });

    test('does not mutate parent traversal', () {
      final base = _g().V().fold();
      final t1 = base.disjunct(['x']);
      final t2 = base.disjunct(['y']);
      expect(t1.gremlinLang.getGremlin(),
          isNot(equals(t2.gremlinLang.getGremlin())));
    });
  });

  // ---------------------------------------------------------------------------
  // GChar wrapper
  // ---------------------------------------------------------------------------
  group('GChar', () {
    test('toStr returns single-character string for ASCII', () {
      expect(const GChar(0x41).toStr(), 'A');
      expect(const GChar(0x7A).toStr(), 'z');
    });

    test('toStr handles supplementary Unicode code points', () {
      expect(const GChar(0x1F600).toStr(), '\u{1F600}');
    });

    test('toString delegates to toStr', () {
      expect(const GChar(0x58).toString(), 'X');
    });
  });

  // ---------------------------------------------------------------------------
  // GraphBinary char round-trip via encodeValue / decodeValue
  // ---------------------------------------------------------------------------
  group('GraphBinary char round-trip', () {
    final writer = GraphBinaryWriter();
    final reader = GraphBinaryReader();

    test('ASCII char encodes and decodes correctly', () {
      final bytes = writer.encodeValue(const GChar(0x41));
      expect(reader.decodeValue(bytes), equals('A'));
    });

    test('BMP Unicode char encodes and decodes correctly', () {
      final bytes = writer.encodeValue(const GChar(0x00E9)); // é
      expect(reader.decodeValue(bytes), equals('é'));
    });

    test('supplementary plane char encodes and decodes correctly', () {
      final bytes = writer.encodeValue(const GChar(0x1F600)); // 😀
      expect(reader.decodeValue(bytes), equals('\u{1F600}'));
    });

    test('type byte is 0x80 (DataType.char)', () {
      final bytes = writer.encodeValue(const GChar(0x41));
      expect(bytes[0], 0x80);
      expect(bytes[1], 0x00); // non-null value flag
    });

    test('encodes as big-endian UTF-32 (4 payload bytes after header)', () {
      // 'A' = 0x00000041
      final bytes = writer.encodeValue(const GChar(0x41));
      // bytes[0] = type 0x80, bytes[1] = flag 0x00, bytes[2..5] = code point
      expect(bytes.length, 6); // 1 type + 1 flag + 4 code point
      expect(bytes[2], 0x00);
      expect(bytes[3], 0x00);
      expect(bytes[4], 0x00);
      expect(bytes[5], 0x41);
    });

    test('null writes unspecifiedNull (0xFE)', () {
      final bytes = writer.encodeValue(null);
      expect(bytes[0], 0xFE);
    });

    test('round-trip survives for all printable ASCII', () {
      for (var cp = 0x20; cp <= 0x7E; cp++) {
        final bytes = writer.encodeValue(GChar(cp));
        final result = reader.decodeValue(bytes);
        expect(result, equals(String.fromCharCode(cp)),
            reason: 'code point 0x${cp.toRadixString(16)} failed');
      }
    });
  });
}
