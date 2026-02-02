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
      assert.strictEqual(gremlinLang.addStep('has', ['active', true]).getGremlin(), "g.has('active', true)");
    });

    it('should handle null arguments', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('property', ['x', null]).getGremlin(), "g.property('x', null)");
    });

    it('should handle multiple number arguments', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('has', ['count', 42]).getGremlin(), "g.has('count', 42)");
    });

    it('should handle string arguments', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('has', ['name', 'josh']).getGremlin(), "g.has('name', 'josh')");
    });

    it('should escape single quotes in strings', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('has', ['x', "it's"]).getGremlin(), "g.has('x', 'it\\'s')");
    });

    it('should escape backslashes in strings', function () {
      const gremlinLang = new GremlinLang();
      assert.strictEqual(gremlinLang.addStep('has', ['x', 'a\\b']).getGremlin(), "g.has('x', 'a\\\\b')");
    });
  });
});