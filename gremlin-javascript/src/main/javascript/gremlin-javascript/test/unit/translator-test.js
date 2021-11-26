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

'use strict';

const assert = require('assert');
const graph = require('../../lib/structure/graph');
const t = require('../../lib/process/traversal');
const Translator = require('../../lib/process/translator');
const graphTraversalModule = require('../../lib/process/graph-traversal');
const __ = graphTraversalModule.statics;

describe('Translator', function () {

  describe('#translate()', function () {
    it('should produce valid script representation from bytecode glv steps', function () {
      const g = new graph.Graph().traversal();
      const script = new Translator('g').translate(g.V().out('created').getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V().out(\'created\')');
    });

    it('should produce valid script representation from bytecode glv steps translating number and text correctly', function () {
      const g = new graph.Graph().traversal();
      const script = new Translator('g').translate(g.V(1).out('created').getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V(1).out(\'created\')');
    });

    it('should produce valid script representation from bytecode glv steps containing parameter bindings', function () {
      const g = new graph.Graph().traversal();
      const script = new Translator('g').translate(g.addV({'name': 'Lilac'}).getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.addV((\'name\', \'Lilac\'))');
    });

    it('should produce valid script representation from bytecode glv steps containing enum', function () {
      const g = new graph.Graph().traversal();
      const script = new Translator('g').translate(g.V().order().by('age', t.order.shuffle).getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V().order().by(\'age\', shuffle)');
    });

    it('should produce valid script representation from bytecode glv steps containing a predicate', function () {
      const g = new graph.Graph().traversal();
      const script = new Translator('g').translate(g.V().hasLabel('person').has('age', t.P.gt(30)).getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V().hasLabel(\'person\').has(\'age\', gt(30))');
    });

    it('should produce valid script representation from bytecode glv steps containing a string predicate', function () {
      const g = new graph.Graph().traversal();
      const script = new Translator('g').translate(g.V().hasLabel('person').has('name', t.TextP.containing("foo")).getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V().hasLabel(\'person\').has(\'name\', containing(\'foo\'))');
    });

    it('should produce valid script representation from bytecode glv steps with child', function () {
      const g = new graph.Graph().traversal();
      const script = new Translator('g').translate(g.V().filter(__.outE('created')).getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V().filter(__.outE(\'created\'))');
    });

    it('should produce valid script representation from bytecode glv steps with embedded child', function () {
      const g = new graph.Graph().traversal();
      const script = new Translator('g').translate(g.V().filter(__.outE('created').filter(__.has('weight'))).getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V().filter(__.outE(\'created\').filter(__.has(\'weight\')))');
    });

    it('should produce valid script representation from bytecode glv steps with embedded children', function () {
      const g = new graph.Graph().traversal();
      const script = new Translator('g').translate(g.V().or(__.has('name', 'a'), __.has('name', 'b')).getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V().or(__.has(\'name\', \'a\'), __.has(\'name\', \'b\'))');
    });

    it('should produce valid script representation from bytecode glv for boolean values', function () {
      const g = new graph.Graph().traversal();
      const script = new Translator('g').translate(g.V().has('male', true).getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V().has(\'male\', true)');
    });

    it('should produce valid script representation from a traversal object', function () {
      const g = new graph.Graph().traversal();
      const script = new Translator('g').translate(g.V().has('male', true));
      assert.ok(script);
      assert.strictEqual(script, 'g.V().has(\'male\', true)');
    });

    it('should produce valid script representation of array as step arg', function () {
      const g = new graph.Graph().traversal();
      const script = new Translator('g').translate(g.V([1, 2, 3]));
      assert.ok(script);
      assert.strictEqual(script, 'g.V([1, 2, 3])');
    });
  });
});
