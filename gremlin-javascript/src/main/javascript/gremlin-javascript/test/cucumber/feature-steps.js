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
const vm = require('vm');
const expect = require('chai').expect;
const graphModule = require('../../lib/structure/graph');
const graphTraversalModule = require('../../lib/process/graph-traversal');
const traversalModule = require('../../lib/process/traversal');
const Graph = graphModule.Graph;
const Path = graphModule.Path;
const __ = graphTraversalModule.statics;

const parsers = [
  [ 'd\\[([\\d.]+)\\]\\.[ilfdm]', toNumeric ],
  [ 'v\\[(.+)\\]', toVertex ],
  [ 'v\\[(.+)\\]\\.id', toVertexId ],
  [ 'v\\[(.+)\\]\\.sid', toVertexIdString ],
  [ 'e\\[(.+)\\]', toEdge ],
  [ 'e\\[(.+)\\]\\.id', toEdgeId ],
  [ 'e\\[(.+)\\]\\.sid', toEdgeIdString ],
  [ 'p\\[(.+)\\]', toPath ],
  [ 'l\\[(.*)\\]', toArray ],
  [ 's\\[(.*)\\]', toArray ],
  [ 'm\\[(.+)\\]', toMap ],
  [ 'c\\[(.+)\\]', toLambda ]
].map(x => [ new RegExp('^' + x[0] + '$'), x[1] ]);

defineSupportCode(function(methods) {
  methods.Given(/^the (.+) graph$/, function (graphName) {
    this.graphName = graphName;
    const data = this.getData();
    this.g = new Graph().traversal().withRemote(data.connection);
    if (graphName === 'empty') {
      return this.cleanEmptyGraph();
    }
  });

  methods.Given('the graph initializer of', function (traversalText) {
    const traversal = vm.runInNewContext(translate(traversalText), getSandbox(this.g, this.parameters));
    return traversal.toList();
  });

  methods.Given('an unsupported test', () => {});

  methods.Given('the traversal of', function (traversalText) {
    this.traversal = vm.runInNewContext(translate(traversalText), getSandbox(this.g, this.parameters));
  });

  methods.Given(/^using the parameter (.+) defined as "(.+)"$/, function (paramName, stringValue) {
    let p = Promise.resolve();
    if (this.graphName === 'empty') {
      p = this.loadEmptyGraphData();
    }
    const self = this;
    return p.then(() => {
      self.parameters[paramName] = parseValue.call(self, stringValue);
    });
  });

  methods.When('iterated to list', function () {
    return this.traversal.toList().then(list => this.result = list);
  });

  methods.When('iterated next', function () {
    return this.traversal.next().then(it => this.result = it.value);
  });

  methods.Then(/^the result should be (\w+)$/, function assertResult(characterizedAs, resultTable) {
    if (characterizedAs === 'empty') {
      expect(this.result).to.be.empty;
      if (typeof resultTable === 'function'){
        return resultTable();
      }
      return;
    }
    const expectedResult = resultTable.rows().map(row => parseRow.call(this, row));
    switch (characterizedAs) {
      case 'ordered':
        console.log('--- ordered', expectedResult);
        expect(this.result).to.have.deep.ordered.members(expectedResult);
        break;
      case 'unordered':
        console.log('--- unordered expected:', expectedResult);
        console.log('--- obtained:', this.result);
        expect(this.result).to.have.deep.members(expectedResult);
        break;
    }
  });

  methods.Then(/^the graph should return (\d+) for count of "(.+)"$/, function (stringCount, traversalText) {
    const traversal = vm.runInNewContext(translate(traversalText), getSandbox(this.g, this.parameters));
    return traversal.toList().then(list => {
      expect(list).to.have.lengthOf(parseInt(stringCount, 10));
    });
  });

  methods.Then(/^the result should have a count of (\d+)$/, function (stringCount) {
    expect(this.result).to.have.lengthOf(parseInt(stringCount, 10));
  });

  methods.Then('nothing should happen because', _ => {});
});

function getSandbox(g, parameters) {
  const sandbox = {
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
  // Pass the parameter to the sandbox
  Object.keys(parameters).forEach(paramName => sandbox[paramName] = parameters[paramName]);
  return sandbox;
}

function translate(traversalText) {
  // Remove escaped chars
  traversalText = traversalText.replace(/\\"/g, '"');
  // Change according to naming convention
  traversalText = traversalText.replace(/\.in\(/g, '.in_(');
  traversalText = traversalText.replace(/\.from\(/g, '.from_(');
  return traversalText;
}

function parseRow(row) {
  return parseValue.call(this, row[0]);
}

function parseValue(stringValue) {
  let extractedValue = null;
  let parser = null;
  for (let item of parsers) {
    let re = item[0];
    let match = re.exec(stringValue);
    if (match && match.length > 1) {
      parser = item[1];
      extractedValue = match[1];
      break;
    }
  }
  return parser !== null ? parser.call(this, extractedValue) : stringValue;
}

function toNumeric(stringValue) {
  return parseFloat(stringValue);
}

function toVertex(name) {
  return this.getData().vertices[name];
}

function toVertexId(name) {
  return toVertex.call(this, name).id;
}

function toVertexIdString(name) {
  return toVertex.call(this, name).id.toString();
}

function toEdge(name) {
  return this.getData().edges[name];
}

function toEdgeId(name) {
  return toEdge.call(this, name).id;
}

function toEdgeIdString(name) {
  return toEdge.call(this, name).id.toString();
}

function toPath(value) {
  const parts = value.split(',');
  return new Path(parts.map(_ => new Array(0)), parts.map(x => parseValue.call(this, x)));
}

function toArray(stringList) {
  if (stringList === '') {
    return new Array(0);
  }
  return stringList.split(',').map(x => parseValue.call(this, x));
}

function toMap() {}

function toLambda() {}