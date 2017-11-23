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

const defineSupportCode = require('cucumber').defineSupportCode;
const assert = require('assert');
const vm = require('vm');
const graphModule = require('../../lib/structure/graph');
const graphTraversalModule = require('../../lib/process/graph-traversal');
const traversalModule = require('../../lib/process/traversal');
const Graph = graphModule.Graph;
const __ = graphTraversalModule.statics;

defineSupportCode(function(methods) {
  methods.Given(/^the (.+) graph$/, function (graphName) {
    if (graphName === 'empty') {
      //TODO
    }
    const data = this.getDataByGraphName(graphName);
    this.graphName = graphName;
    this.g = new Graph().traversal().withRemote(data.connection)
  });

  methods.Given('the graph initializer of', function () {
    //TODO
  });

  methods.Given('an unsupported test', () => {});

  methods.Given('the traversal of', function (traversalText) {
    this.traversal = vm.runInNewContext(translate(traversalText), getSandbox(this.g));
  });

  methods.Given(/^$/, function (paramName, stringValue) {
    //TODO: Add parameter
  });

  methods.When('iterated to list', function () {
    return this.traversal.toList().then(list => this.result = list);
  });

  methods.When('iterated next', function () {
    return this.traversal.next().then(it => this.result = it.value);
  });

  methods.Then(/^the result should be (\w+)$/, function (characterizedAs, resultTable) {
    switch (characterizedAs) {
      case 'empty':
        assert.strictEqual(0, result.length);
        break;
      case 'ordered':
        const expectedResult = resultTable.rows().map(parseRow);
        console.log('--- ordered', expectedResult);
        break;
    }
    //TODO
    if (typeof resultTable === 'function'){
      return resultTable();
    }
  });

  methods.Then(/^the graph should return (\d+) for count of (.+)$/, function (stringCount, traversalString) {

  });

  methods.Then(/^the result should have a count of (\d+)$/, function (stringCount) {

  });

  methods.Then('nothing should happen because', () => {});
});

function getSandbox(g) {
  return {
    g: g,
    __: __,
    Cardinality: traversalModule.cardinality,
    Column: traversalModule.column,
    Direction: traversalModule.direction,
    Order: traversalModule.order,
    P: traversalModule.P,
    Pick: traversalModule.pick,
    Scope: traversalModule.scope,
    Operator: traversalModule.operator,
    T: traversalModule.t,
  };
}

function translate(traversalText) {
  traversalText = traversalText.replace('.in(', '.in_(');
  traversalText = traversalText.replace('.from(', '.from_(');
  return traversalText;
}

function parseRow(row) {
  return row[0];
}