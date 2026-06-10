// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:gremlin_dart/gremlin_dart.dart';

void main() {
  group('GremlinLang', () {
    test('builds basic traversal string', () {
      final gl = GremlinLang();
      gl.addStep('V');
      gl.addStep('hasLabel', ['person']);
      expect(gl.getGremlin(), 'g.V().hasLabel(\'person\')');
    });

    test('serialises int32 without suffix', () {
      final gl = GremlinLang();
      gl.addStep('has', ['age', P.gt(30)]);
      expect(gl.getGremlin(), contains("gt(30)"));
    });

    test('serialises long with L suffix', () {
      final gl = GremlinLang();
      gl.addStep('has', ['count', P.gt(GLong(9999999999))]);
      expect(gl.getGremlin(), contains('9999999999L'));
    });

    test('serialises string with single quotes', () {
      final gl = GremlinLang();
      gl.addStep('hasLabel', ['person']);
      expect(gl.getGremlin(), contains("'person'"));
    });

    test('serialises list', () {
      final gl = GremlinLang();
      gl.addStep('hasLabel', [['person', 'software']]);
      expect(gl.getGremlin(), contains("['person','software']"));
    });

    test('serialises null', () {
      final gl = GremlinLang();
      gl.addStep('has', ['age', null]);
      expect(gl.getGremlin(), contains('null'));
    });

    test('serialises EnumValue', () {
      final gl = GremlinLang();
      gl.addStep('order', [order.asc]);
      expect(gl.getGremlin(), contains('Order.asc'));
    });

    test('serialises P.within', () {
      final gl = GremlinLang();
      gl.addStep('has', ['age', P.within([1, 2, 3])]);
      expect(gl.getGremlin(), contains("within([1,2,3])"));
    });

  });

  group('P predicates', () {
    test('eq', () => expect(P.eq(1).toString(), "eq(1)"));
    test('neq', () => expect(P.neq(1).toString(), "neq(1)"));
    test('gt', () => expect(P.gt(5).toString(), "gt(5)"));
    test('between', () => expect(P.between(1, 10).toString(), "between(1, 10)"));
    test('within list', () => expect(P.within([1, 2]).toString(), contains('within')));
  });

  group('TextP predicates', () {
    test('containing', () => expect(TextP.containing('foo').toString(), "containing('foo')"));
    test('startingWith', () => expect(TextP.startingWith('bar').toString(), "startingWith('bar')"));
  });

  group('RequestMessage', () {
    test('build creates message with gremlin field', () {
      final msg = RequestMessage.build('g.V()')
          .addG('g')
          .addBulkResults(true)
          .create();
      expect(msg.gremlin, 'g.V()');
      expect(msg.g, 'g');
      expect(msg.bulkResults, true);
      expect(msg.language, 'gremlin-lang');
    });

    test('toJson includes all set fields', () {
      final msg = RequestMessage.build('g.V()')
          .addG('g')
          .addTimeoutMillis(3000)
          .create();
      final json = msg.toJson();
      expect(json['gremlin'], 'g.V()');
      expect(json['g'], 'g');
      expect(json['timeoutMs'], 3000);
    });

  });

  group('GraphTraversal DSL', () {
    late GraphTraversalSource g;

    setUp(() {
      // Build a source without a remote connection for string-generation tests
      g = GraphTraversalSource(Graph(), TraversalStrategies());
    });

    test('V() generates g.V()', () {
      expect(g.V().toString(), 'g.V()');
    });

    test('V().hasLabel produces correct string', () {
      expect(g.V().hasLabel('person').toString(), "g.V().hasLabel('person')");
    });

    test('chained steps produce correct string', () {
      final t = g.V().out(['knows']).values(['name']);
      expect(t.toString(), "g.V().out('knows').values('name')");
    });

    test('nested traversal via __', () {
      final t = g.V().repeat(Anon.out(['knows'])).times(2);
      expect(t.toString(), contains("repeat(__.out('knows'))"));
    });

    test('has with P predicate', () {
      final t = g.V().has('age', P.gt(30));
      expect(t.toString(), contains("has('age',gt(30))"));
    });
  });

  group('GremlinLang.valueToGremlinLiteral', () {
    test('GShort serialises with S suffix', () {
      expect(GremlinLang.valueToGremlinLiteral(GShort(42)), '42S');
      expect(GremlinLang.valueToGremlinLiteral(GShort(-1)), '-1S');
    });

    test('GByte serialises with B suffix', () {
      expect(GremlinLang.valueToGremlinLiteral(GByte(5)), '5B');
      expect(GremlinLang.valueToGremlinLiteral(GByte(0)), '0B');
    });

    test('GInt serialises without suffix', () {
      expect(GremlinLang.valueToGremlinLiteral(GInt(10)), '10');
      expect(GremlinLang.valueToGremlinLiteral(GInt(-7)), '-7');
    });

    test('DateTime serialises as datetime(...) in UTC', () {
      final dt = DateTime.utc(2024, 6, 1, 12, 34, 56);
      final result = GremlinLang.valueToGremlinLiteral(dt);
      expect(result, 'datetime("2024-06-01T12:34:56.000Z")');
    });

    test('Uint8List serialises as Binary(base64)', () {
      final bytes = Uint8List.fromList([0x01, 0x02, 0x03]);
      expect(GremlinLang.valueToGremlinLiteral(bytes), 'Binary("AQID")');
    });

    test('empty Uint8List serialises as Binary("")', () {
      expect(GremlinLang.valueToGremlinLiteral(Uint8List(0)), 'Binary("")');
    });

    test('Set with one element serialises as {item}', () {
      expect(GremlinLang.valueToGremlinLiteral({1}), '{1}');
    });

    test('empty Set serialises as {}', () {
      expect(GremlinLang.valueToGremlinLiteral(<int>{}), '{}');
    });

    test('empty Map serialises as [:]', () {
      expect(GremlinLang.valueToGremlinLiteral(<String, dynamic>{}), '[:]');
    });
  });

  group('BasicAuth', () {
    test('headerValue encodes username:password as Base64 Basic', () {
      final auth = BasicAuth(username: 'alice', password: 'secret');
      // 'alice:secret' → base64 → YWxpY2U6c2VjcmV0
      expect(auth.headerValue, 'Basic YWxpY2U6c2VjcmV0');
    });

    test('headerValue starts with Basic for any credentials', () {
      final auth = BasicAuth(username: 'user@example.com', password: 'p@ss!');
      expect(auth.headerValue, startsWith('Basic '));
    });
  });
}
