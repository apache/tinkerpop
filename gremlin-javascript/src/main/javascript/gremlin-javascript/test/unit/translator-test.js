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
import { Graph } from '../../lib/structure/graph.js';
import { order, P, TextP } from '../../lib/process/traversal.js';
import Translator from '../../lib/process/translator.js';
import { statics } from '../../lib/process/graph-traversal.js';
const __ = statics;

describe('Translator', function () {

  describe('#translate()', function () {
    it('should produce valid script representation from bytecode glv steps', function () {
      const g = new Graph().traversal();
      const script = new Translator('g').translate(g.V().out('created').getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V().out(\'created\')');
    });

    it('should produce valid script representation from bytecode glv steps translating number and text correctly', function () {
      const g = new Graph().traversal();
      const script = new Translator('g').translate(g.V(1).out('created').getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V(1).out(\'created\')');
    });

    it('should produce valid script representation from bytecode glv steps containing parameter bindings', function () {
      const g = new Graph().traversal();
      const script = new Translator('g').translate(g.addV({'name': 'Lilac'}).getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.addV((\'name\', \'Lilac\'))');
    });

    it('should produce valid script representation from bytecode glv steps containing enum', function () {
      const g = new Graph().traversal();
      const script = new Translator('g').translate(g.V().order().by('age', order.shuffle).getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V().order().by(\'age\', shuffle)');
    });

    it('should produce valid script representation from bytecode glv steps containing a predicate', function () {
      const g = new Graph().traversal();
      const script = new Translator('g').translate(g.V().hasLabel('person').has('age', P.gt(30)).getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V().hasLabel(\'person\').has(\'age\', gt(30))');
    });

    it('should produce valid script representation from bytecode glv steps containing a string predicate', function () {
      const g = new Graph().traversal();
      const script = new Translator('g').translate(g.V().hasLabel('person').has('name', TextP.containing("foo")).getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V().hasLabel(\'person\').has(\'name\', containing(\'foo\'))');
    });

    it('should produce valid script representation from bytecode glv steps with child', function () {
      const g = new Graph().traversal();
      const script = new Translator('g').translate(g.V().filter(__.outE('created')).getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V().filter(__.outE(\'created\'))');
    });

    it('should produce valid script representation from bytecode glv steps with embedded child', function () {
      const g = new Graph().traversal();
      const script = new Translator('g').translate(g.V().filter(__.outE('created').filter(__.has('weight'))).getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V().filter(__.outE(\'created\').filter(__.has(\'weight\')))');
    });

    it('should produce valid script representation from bytecode glv steps with embedded children', function () {
      const g = new Graph().traversal();
      const script = new Translator('g').translate(g.V().or(__.has('name', 'a'), __.has('name', 'b')).getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V().or(__.has(\'name\', \'a\'), __.has(\'name\', \'b\'))');
    });

    it('should produce valid script representation from bytecode glv for boolean values', function () {
      const g = new Graph().traversal();
      const script = new Translator('g').translate(g.V().has('male', true).getBytecode());
      assert.ok(script);
      assert.strictEqual(script, 'g.V().has(\'male\', true)');
    });

    it('should produce valid script representation from a traversal object', function () {
      const g = new Graph().traversal();
      const script = new Translator('g').translate(g.V().has('male', true));
      assert.ok(script);
      assert.strictEqual(script, 'g.V().has(\'male\', true)');
    });

    it('should produce valid script representation of array as step arg', function () {
      const g = new Graph().traversal();
      const script = new Translator('g').translate(g.V([1, 2, 3]));
      assert.ok(script);
      assert.strictEqual(script, 'g.V([1, 2, 3])');
    });

    it('should translate null', function () {
      const g = new Graph().traversal();
      const script = new Translator('g').translate(g.addV('test').property('empty', null));
      assert.ok(script);
      assert.strictEqual(script, 'g.addV(\'test\').property(\'empty\', null)');
    });

    it('should translate undefined to null', function () {
      const g = new Graph().traversal();
      const script = new Translator('g').translate(g.addV('test').property('empty', undefined));
      assert.ok(script);
      assert.strictEqual(script, 'g.addV(\'test\').property(\'empty\', null)');
    });

    it('should properly escape quotes in string literals', function () {
      const g = new Graph().traversal();
      const script = new Translator('g').translate(g.addV('test').property('quotes', "some \"quotes' in the middle."));
      assert.ok(script);
      assert.strictEqual(script, 'g.addV(\'test\').property(\'quotes\', \'some "quotes\\\' in the middle.\')');
    });

    it('should properly escape quotes in Object values', function () {
      const g = new Graph().traversal();
      const o = {key: "some \"quotes' in the middle."}
      const script = new Translator('g').translate(g.inject(o));
      assert.ok(script);
      assert.strictEqual(script, 'g.inject((\'key\', \'some "quotes\\\' in the middle.\'))');
    });
  });
});
