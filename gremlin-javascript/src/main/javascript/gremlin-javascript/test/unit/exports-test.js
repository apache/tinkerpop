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
'use strict';

var assert = require('assert');
var glvModule = require('../../');

describe('API', function () {
  it('should export fields under process', function () {
    assert.ok(glvModule);
    assert.ok(glvModule.process);
    assert.strictEqual(typeof glvModule.process.Bytecode, 'function');
    assert.strictEqual(typeof glvModule.process.EnumValue, 'function');
    assert.strictEqual(typeof glvModule.process.P, 'function');
    assert.strictEqual(typeof glvModule.process.Traversal, 'function');
    assert.strictEqual(typeof glvModule.process.TraversalSideEffects, 'function');
    assert.strictEqual(typeof glvModule.process.TraversalStrategies, 'function');
    assert.strictEqual(typeof glvModule.process.TraversalStrategy, 'function');
    assert.strictEqual(typeof glvModule.process.Traverser, 'function');
    assert.strictEqual(typeof glvModule.process.GraphTraversal, 'function');
    assert.strictEqual(typeof glvModule.process.GraphTraversalSource, 'function');
    assert.strictEqual(typeof glvModule.process.barrier, 'object');
    assert.strictEqual(typeof glvModule.process.cardinality, 'object');
    assert.strictEqual(typeof glvModule.process.column, 'object');
    assert.strictEqual(typeof glvModule.process.direction, 'object');
    assert.strictEqual(typeof glvModule.process.direction.both, 'object');
    assert.strictEqual(glvModule.process.direction.both.elementName, 'BOTH');
    assert.strictEqual(typeof glvModule.process.operator, 'object');
    assert.strictEqual(typeof glvModule.process.order, 'object');
    assert.strictEqual(typeof glvModule.process.pop, 'object');
    assert.strictEqual(typeof glvModule.process.scope, 'object');
    assert.strictEqual(typeof glvModule.process.t, 'object');
    assert.ok(glvModule.process.statics);
  });
  it('should expose fields under structure', function () {
    assert.ok(glvModule.structure);
    assert.ok(glvModule.structure.io);
    assert.strictEqual(typeof glvModule.structure.io.GraphSONReader, 'function');
    assert.strictEqual(typeof glvModule.structure.io.GraphSONWriter, 'function');
    assert.strictEqual(typeof glvModule.structure.Edge, 'function');
    assert.strictEqual(typeof glvModule.structure.Graph, 'function');
    assert.strictEqual(typeof glvModule.structure.Path, 'function');
    assert.strictEqual(typeof glvModule.structure.Property, 'function');
    assert.strictEqual(typeof glvModule.structure.Vertex, 'function');
    assert.strictEqual(typeof glvModule.structure.VertexProperty, 'function');
  });
  it('should expose fields under driver', function () {
    assert.ok(glvModule.driver);
    assert.strictEqual(typeof glvModule.driver.RemoteConnection, 'function');
    assert.strictEqual(typeof glvModule.driver.RemoteStrategy, 'function');
    assert.strictEqual(typeof glvModule.driver.RemoteTraversal, 'function');
  });
});