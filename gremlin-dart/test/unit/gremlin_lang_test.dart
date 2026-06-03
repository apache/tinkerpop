// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

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

    test('getParametersAsString returns [:]  for empty', () {
      final gl = GremlinLang();
      expect(gl.getParametersAsString(), '[:]');
    });

    test('getParametersAsString includes g binding', () {
      final gl = GremlinLang();
      gl.addG('g');
      expect(gl.getParametersAsString(), contains("'g':'g'"));
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

    test('throws on mixed bindings', () {
      expect(
        () => RequestMessage.build('g.V()')
            .addBinding('x', 1)
            .addBindingsString('["y":2]'),
        throwsStateError,
      );
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
}
