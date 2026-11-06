/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import assert from 'assert';
import * as glvModule from '../../lib/index.js';

describe('API', function () {
  it('should export fields under process', function () {
    assert.ok(glvModule);
    assert.ok(glvModule.process);
    assert.strictEqual(typeof glvModule.process.GremlinLang, 'function');
    assert.strictEqual(typeof glvModule.process.GValue, 'function');
    assert.strictEqual(typeof glvModule.process.EnumValue, 'function');
    assert.strictEqual(typeof glvModule.process.P, 'function');
    assert.strictEqual(typeof glvModule.process.Traversal, 'function');
    assert.strictEqual(typeof glvModule.process.TraversalSideEffects, 'function');
    assert.strictEqual(typeof glvModule.process.TraversalStrategies, 'function');
    assert.strictEqual(typeof glvModule.process.TraversalStrategy, 'function');
    assert.strictEqual(typeof glvModule.process.Traverser, 'function');
    assert.strictEqual(typeof glvModule.process.GraphTraversal, 'function');
    assert.strictEqual(typeof glvModule.process.GraphTraversalSource, 'function');
    validateConstructor(glvModule.process, 'CardinalityValue');
    assert.strictEqual(typeof glvModule.process.CardinalityValue.single, 'function');
    assert.strictEqual(typeof glvModule.process.CardinalityValue.list, 'function');
    assert.strictEqual(typeof glvModule.process.CardinalityValue.set, 'function');
    for (const name of enumNames) {
      assert.strictEqual(typeof glvModule.process[name], 'object', name);
    }
    assert.strictEqual(glvModule.process.Direction.BOTH.elementName, 'BOTH');
    assert.strictEqual(glvModule.process.Direction.from_, glvModule.process.Direction.OUT);
    assert.strictEqual(glvModule.process.Direction.to, glvModule.process.Direction.IN);
    assert.strictEqual(glvModule.process.GType.string.elementName, 'STRING');
    assert.strictEqual(typeof glvModule.process.__.out, 'function');
    validateConstructor(glvModule.process, 'IO');
    validateConstructor(glvModule.process, 'AnonymousTraversalSource');
    assert.strictEqual(typeof glvModule.process.traversal, 'function');
  });
  it('should expose fields under structure', function () {
    assert.ok(glvModule.structure);
    assert.strictEqual(typeof glvModule.structure.Edge, 'function');
    assert.strictEqual(typeof glvModule.structure.Graph, 'function');
    assert.strictEqual(typeof glvModule.structure.Path, 'function');
    assert.strictEqual(typeof glvModule.structure.Property, 'function');
    assert.strictEqual(typeof glvModule.structure.Vertex, 'function');
    assert.strictEqual(typeof glvModule.structure.VertexProperty, 'function');
    assert.strictEqual(typeof glvModule.structure.toLong, 'function');
    assert.strictEqual(typeof glvModule.structure.toInt, 'function');
    assert.strictEqual(typeof glvModule.structure.toFloat, 'function');
    assert.strictEqual(typeof glvModule.structure.toDouble, 'function');
    assert.strictEqual(typeof glvModule.structure.toShort, 'function');
    assert.strictEqual(typeof glvModule.structure.toByte, 'function');
    assert.strictEqual(typeof glvModule.structure.Int, 'function');
    assert.strictEqual(typeof glvModule.structure.Float, 'function');
    assert.strictEqual(typeof glvModule.structure.Double, 'function');
    assert.strictEqual(typeof glvModule.structure.Short, 'function');
    assert.strictEqual(typeof glvModule.structure.Byte, 'function');
    assert.strictEqual(typeof glvModule.structure.Long, 'function');
    assert.strictEqual(typeof glvModule.structure.unwrap, 'function');
  });
  it('should expose fields under driver', function () {
    assert.ok(glvModule.driver);
    validateConstructor(glvModule.driver, 'RemoteConnection');
    validateConstructor(glvModule.driver, 'RemoteStrategy');
    validateConstructor(glvModule.driver, 'RemoteTraversal');
    validateConstructor(glvModule.driver, 'DriverRemoteConnection');
    validateConstructor(glvModule.driver, 'Client');
    validateConstructor(glvModule.driver, 'ResultSet');
    assert.strictEqual(typeof glvModule.driver.auth.basic, 'function');
    assert.strictEqual(typeof glvModule.driver.auth.sigv4, 'function');
  });
  it('should not expose the pre-4.0 lowercase names', function () {
    for (const name of ['barrier', 'cardinality', 'column', 'direction', 'dt', 'merge', 'operator', 'order', 'pick',
                        'pop', 'scope', 't', 'statics', 'withOptions']) {
      assert.strictEqual(glvModule.process[name], undefined, name);
      assert.strictEqual(glvModule[name], undefined, name);
    }
    assert.strictEqual(glvModule.process.Direction.out, undefined);
  });
  it('should expose every namespaced field as a named export', function () {
    for (const ns of ['driver', 'process', 'structure']) {
      for (const [name, value] of Object.entries(glvModule[ns])) {
        assert.strictEqual(glvModule[name], value, `${ns}.${name}`);
      }
    }
  });
  it('should group every named export under a namespace', function () {
    const namespaces = ['driver', 'process', 'structure'];
    for (const [name, value] of Object.entries(glvModule)) {
      if (name === 'default' || namespaces.includes(name)) continue;
      assert.ok(namespaces.some((ns) => glvModule[ns][name] === value), name);
    }
  });
  it('should expose every named export on the default export', function () {
    const named = Object.keys(glvModule).filter((name) => name !== 'default');
    assert.deepStrictEqual(Object.keys(glvModule.default).sort(), named.sort());
    for (const name of named) {
      assert.strictEqual(glvModule.default[name], glvModule[name], name);
    }
  });
});

const enumNames = ['Barrier', 'Cardinality', 'Column', 'Direction', 'DT', 'GType', 'Merge', 'Operator', 'Order',
                   'Pick', 'Pop', 'Scope', 'T', 'WithOptions'];

function validateConstructor(parent, name) {
  assert.strictEqual(typeof parent[name], 'function');
  assert.strictEqual(parent[name].name, name);
}
