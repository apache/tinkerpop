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

    test('multiple strategy names are unquoted identifiers separated by commas', () {
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
      expect(base.gremlinLang.getGremlin(),
          isNot(contains('withoutStrategies')));
    });

    test('can be chained with traversal steps', () {
      final t = _g().withoutStrategies(['ConnectiveStrategy']).V();
      expect(t.gremlinLang.getGremlin(), contains('V()'));
    });

    test('can be combined with withStrategies', () {
      final gs = _g()
          .withStrategies([ReadOnlyStrategy()])
          .withoutStrategies(['EarlyLimitStrategy']);
      final gremlin = gs.gremlinLang.getGremlin();
      expect(gremlin, contains('withStrategies'));
      expect(gremlin,
          contains('withoutStrategies(EarlyLimitStrategy)'));
    });

    test('empty list produces no withoutStrategies step', () {
      final gs = _g().withoutStrategies([]);
      final gremlin = gs.gremlinLang.getGremlin();
      expect(gremlin, isNot(contains('withoutStrategies')));
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
