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
 * @author Serhiy Salo
 */
'use strict';

const assert = require('assert');
const graph = require('../../lib/structure/graph');
const graphTraversalModule = require('../../lib/process/graph-traversal');
const __ = graphTraversalModule.statics;
const { ReadOnlyStrategy } = require('../../lib/process/traversal-strategy');

describe('Bytecode', () => {

    describe('#toString()', () => {
        it('should produce valid string representation of bytecode', () => {
            const g = new graph.Graph().traversal();
            const bytecode = g.V().hasLabel("person").has("name", "peter");
            assert.ok(bytecode);
            assert.strictEqual(bytecode.toString(), '[[],[["V"],["hasLabel","person"],["has","name","peter"]]]');
        });
        it('should produce valid string representation of bytecode with reference to inner steps', () => {
            const g = new graph.Graph().traversal();
            const bytecode = g.V().hasLabel("airport").where(__.outE("route").hasId(7753));
            assert.ok(bytecode);
            assert.strictEqual(bytecode.toString(), '[[],[["V"],["hasLabel","airport"],["where",[["outE","route"],["hasId",7753]]]]]');
        });
        it('should produce valid string representation of bytecode with multiple parameters', () => {
            const g = new graph.Graph().traversal();
            const bytecode = g.V().has('person','name','marko');
            assert.ok(bytecode);
            assert.strictEqual(bytecode.toString(), '[[],[["V"],["has","person","name","marko"]]]');
        });
        it('should produce valid string representation of bytecode with strategies', () => {
            const g = new graph.Graph().traversal();
            const bytecode = g.with_('enabled').withStrategies(new ReadOnlyStrategy()).V();
            assert.ok(bytecode);
            assert.strictEqual(bytecode.toString(), '[[["withStrategies",{"fqcn":"org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy","configuration":{"enabled":true}}],["withStrategies",{"fqcn":"org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy","configuration":{}}]],[["V"]]]');
        });
    });
});