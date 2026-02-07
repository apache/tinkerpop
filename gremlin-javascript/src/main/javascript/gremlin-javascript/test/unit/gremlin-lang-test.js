/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

import assert from 'assert';
import GremlinLang from '../../lib/process/gremlin-lang.js';
import { P, TextP, order, t } from '../../lib/process/traversal.js';
import { OptionsStrategy, ReadOnlyStrategy } from '../../lib/process/traversal-strategy.js';
import { Long, toLong } from '../../lib/utils.js';

describe('GremlinLang', function () {
  describe('#getGremlin()', function () {
    it('should return "g" for empty traversal with default prefix', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.getGremlin(), 'g');
    });

    it('should return custom prefix for empty traversal', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.getGremlin('__'), '__');
    });
  });

  describe('#addStep()', function () {
    it('should add single step', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').getGremlin(), 'g.V()');
    });

    it('should chain multiple steps', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').addStep('count').getGremlin(), 'g.V().count()');
    });

    it('should handle number arguments', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V', [1]).getGremlin(), 'g.V(1)');
    });

    it('should handle boolean arguments', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').addStep('has', ['active', true]).getGremlin(), "g.V().has('active', true)");
    });

    it('should handle null arguments', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').addStep('property', ['x', null]).getGremlin(), "g.V().property('x', null)");
    });

    it('should handle multiple number arguments', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').addStep('has', ['count', 42]).getGremlin(), "g.V().has('count', 42)");
    });

    it('should handle string arguments', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').addStep('has', ['name', 'josh']).getGremlin(), "g.V().has('name', 'josh')");
    });

    it('should escape single quotes in strings', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').addStep('has', ['x', "it's"]).getGremlin(), "g.V().has('x', 'it\\'s')");
    });

    it('should escape backslashes in strings', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').addStep('has', ['x', 'a\\b']).getGremlin(), "g.V().has('x', 'a\\\\b')");
    });

    it('should handle array arguments', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [[1, 2, 3]]).getGremlin(), 'g.inject([1, 2, 3])');
    });

    it('should handle empty array', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [[]]).getGremlin(), 'g.inject([])');
    });

    it('should handle nested arrays', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [[[1, 2], [3, 4]]]).getGremlin(), 'g.inject([[1, 2], [3, 4]])');
    });

    it('should handle array with mixed types', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [['a', 1, true, null]]).getGremlin(), "g.inject(['a', 1, true, null])");
    });

    it('should handle NaN', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [NaN]).getGremlin(), 'g.inject(NaN)');
    });

    it('should handle Infinity', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [Infinity]).getGremlin(), 'g.inject(+Infinity)');
    });

    it('should handle -Infinity', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [-Infinity]).getGremlin(), 'g.inject(-Infinity)');
    });

    it('should handle Long values', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V', [new Long('9007199254740993')]).getGremlin(), 'g.V(9007199254740993)');
    });

    it('should handle toLong values', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V', [toLong('9223372036854775807')]).getGremlin(), 'g.V(9223372036854775807)');
    });

    it('should handle Date values', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [new Date('2018-03-21T08:35:44.741Z')]).getGremlin(), "g.inject(datetime('2018-03-21T08:35:44.741Z'))");
    });

    it('should handle Date in has step', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').addStep('has', ['created', new Date('2024-01-01T00:00:00.000Z')]).getGremlin(), "g.V().has('created', datetime('2024-01-01T00:00:00.000Z'))");
    });
  });

  describe('clone support', function () {
    it('should create independent copy', function () {
      const original = new GremlinLang();
      original.addStep('V').addStep('count');
      const clone = new GremlinLang(original);
      assert.strictEqual(clone.getGremlin(), original.getGremlin());
    });

    it('should not affect original when modifying clone', function () {
      const original = new GremlinLang();
      original.addStep('V');
      const clone = new GremlinLang(original);
      clone.addStep('count');
      assert.strictEqual(original.getGremlin(), 'g.V()');
      assert.strictEqual(clone.getGremlin(), 'g.V().count()');
    });

    it('should not affect clone when modifying original', function () {
      const original = new GremlinLang();
      original.addStep('V');
      const clone = new GremlinLang(original);
      original.addStep('count');
      assert.strictEqual(clone.getGremlin(), 'g.V()');
      assert.strictEqual(original.getGremlin(), 'g.V().count()');
    });
  });

  describe('P and TextP predicates', function () {
    it('should handle P.gt predicate', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').addStep('has', ['age', P.gt(5)]).getGremlin(), "g.V().has('age', gt(5))");
    });

    it('should handle P.within with array', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').addStep('has', ['age', P.within([1, 2, 3])]).getGremlin(), "g.V().has('age', within([1, 2, 3]))");
    });

    it('should handle P.within with empty array', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').addStep('has', ['age', P.within([])]).getGremlin(), "g.V().has('age', within([]))");
    });

    it('should handle TextP.containing predicate', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').addStep('has', ['name', TextP.containing('foo')]).getGremlin(), "g.V().has('name', containing('foo'))");
    });

    it('should handle chained P predicates', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').addStep('has', ['age', P.gt(5).and(P.lt(10))]).getGremlin(), "g.V().has('age', gt(5).and(lt(10)))");
    });
  });

  describe('EnumValue support', function () {
    it('should handle order enum in by step', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').addStep('order').addStep('by', [order.shuffle]).getGremlin(), 'g.V().order().by(shuffle)');
    });

    it('should handle t.label enum', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').addStep('values', [t.label]).getGremlin(), 'g.V().values(label)');
    });

    it('should handle enum in complex step', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('V').addStep('order').addStep('by', ['name', order.desc]).getGremlin(), "g.V().order().by('name', desc)");
    });
  });

  describe('Object/Map support', function () {
    it('should handle empty object', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [{}]).getGremlin(), 'g.inject([:])');
    });

    it('should handle simple object', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [{name: 'josh'}]).getGremlin(), "g.inject(['name':'josh'])");
    });

    it('should handle object with multiple entries', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [{name: 'josh', age: 32}]).getGremlin(), "g.inject(['name':'josh', 'age':32])");
    });

    it('should handle nested object', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [{outer: {inner: 'value'}}]).getGremlin(), "g.inject(['outer':['inner':'value']])");
    });

    it('should handle object with mixed value types', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [{str: 'a', num: 1, bool: true}]).getGremlin(), "g.inject(['str':'a', 'num':1, 'bool':true])");
    });
  });

  describe('Set and Map support', function () {
    it('should handle Set with numbers', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [new Set([1, 2, 3])]).getGremlin(), 'g.inject({1, 2, 3})');
    });

    it('should handle empty Set', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [new Set()]).getGremlin(), 'g.inject({})');
    });

    it('should handle Set with strings', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [new Set(['a', 'b'])]).getGremlin(), "g.inject({'a', 'b'})");
    });

    it('should handle Map with string key', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [new Map([['name', 'josh']])]).getGremlin(), "g.inject(['name':'josh'])");
    });

    it('should handle empty Map', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [new Map()]).getGremlin(), 'g.inject([:])');
    });

    it('should handle Map with enum key', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('inject', [new Map([[t.label, 'person']])]).getGremlin(), "g.inject([label:'person'])");
    });
  });

  describe('addSource', function () {
    it('should add withBulk source', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addSource('withBulk', [false]).getGremlin(), 'g.withBulk(false)');
    });

    it('should add withSack source', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addSource('withSack', [1]).getGremlin(), 'g.withSack(1)');
    });

    it('should chain source followed by step', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addSource('withBulk', [false]).addStep('V').getGremlin(), 'g.withBulk(false).V()');
    });
  });

  describe('OptionsStrategy extraction', function () {
    it('should store OptionsStrategy and not serialize it', function () {
      const gremlinLang = new GremlinLang();
      const optionsStrategy = new OptionsStrategy({timeout: 1000});
      gremlinLang.addSource('withStrategies', [optionsStrategy]);
      assert.strictEqual(gremlinLang.getGremlin(), 'g');
      assert.strictEqual(gremlinLang.getOptionsStrategies().length, 1);
      assert.strictEqual(gremlinLang.getOptionsStrategies()[0], optionsStrategy);
    });

    it('should serialize non-OptionsStrategy', function () {
      const gremlinLang = new GremlinLang();
      gremlinLang.addSource('withStrategies', [new ReadOnlyStrategy()]);
      assert.strictEqual(gremlinLang.getGremlin(), 'g.withStrategies(ReadOnlyStrategy)');
    });

    it('should handle mixed strategies', function () {
      const gremlinLang = new GremlinLang();
      const optionsStrategy = new OptionsStrategy({timeout: 1000});
      gremlinLang.addSource('withStrategies', [optionsStrategy, new ReadOnlyStrategy()]);
      assert.strictEqual(gremlinLang.getGremlin(), 'g.withStrategies(ReadOnlyStrategy)');
      assert.strictEqual(gremlinLang.getOptionsStrategies().length, 1);
      assert.strictEqual(gremlinLang.getOptionsStrategies()[0], optionsStrategy);
    });

    it('should clone optionsStrategies array', function () {
      const original = new GremlinLang();
      const optionsStrategy = new OptionsStrategy({timeout: 1000});
      original.addSource('withStrategies', [optionsStrategy]);
      const clone = new GremlinLang(original);
      assert.strictEqual(clone.getOptionsStrategies().length, 1);
      assert.strictEqual(clone.getOptionsStrategies()[0], optionsStrategy);
    });
  });
});