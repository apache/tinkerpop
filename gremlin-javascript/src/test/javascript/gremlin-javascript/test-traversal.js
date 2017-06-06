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

/**
 * @author Jorge Bay Gondra
 */
(function defineTestCases() {
  "use strict";

  var helper = loadModule.call(this, './helper.js');
  var assert = helper.assert;
  var graph = helper.loadLibModule.call(this, 'structure/graph.js');
  var t = helper.loadLibModule.call(this, 'process/traversal.js');

  [
    function testBytecode1() {
      var g = new graph.Graph().traversal();
      var bytecode = g.V().out('created').getBytecode();
      assert.ok(bytecode);
      assert.strictEqual(bytecode.sourceInstructions.length, 0);
      assert.strictEqual(bytecode.stepInstructions.length, 2);
      assert.strictEqual(bytecode.stepInstructions[0][0], 'V');
      assert.strictEqual(bytecode.stepInstructions[1][0], 'out');
      assert.strictEqual(bytecode.stepInstructions[1][1], 'created');
    },
    function testBytecode2() {
      var g = new graph.Graph().traversal();
      var bytecode = g.V().order().by('age', t.order.decr).getBytecode();
      assert.ok(bytecode);
      assert.strictEqual(bytecode.sourceInstructions.length, 0);
      assert.strictEqual(bytecode.stepInstructions.length, 3);
      assert.strictEqual(bytecode.stepInstructions[0][0], 'V');
      assert.strictEqual(bytecode.stepInstructions[1][0], 'order');
      assert.strictEqual(bytecode.stepInstructions[2][0], 'by');
      assert.strictEqual(bytecode.stepInstructions[2][1], 'age');
      assert.strictEqual(typeof bytecode.stepInstructions[2][2], 'object');
      assert.strictEqual(bytecode.stepInstructions[2][2].typeName, 'Order');
      assert.strictEqual(bytecode.stepInstructions[2][2].elementName, 'decr');
    }
  ].forEach(function (testCase) {
    testCase.call(null);
  });

  function loadModule(moduleName) {
    if (typeof require !== 'undefined') {
      return require(moduleName);
    }
    if (typeof load !== 'undefined') {
      return load(__DIR__ + moduleName);
    }
    throw new Error('No module loader was found');
  }
}).call(this);