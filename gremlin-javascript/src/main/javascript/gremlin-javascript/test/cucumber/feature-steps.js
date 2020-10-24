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

const {Given, Then, When} = require('cucumber');
const vm = require('vm');
const expect = require('chai').expect;
const util = require('util');
const graphModule = require('../../lib/structure/graph');
const graphTraversalModule = require('../../lib/process/graph-traversal');
const traversalModule = require('../../lib/process/traversal');
const utils = require('../../lib/utils');
const traversal = require('../../lib/process/anonymous-traversal').traversal;
const Path = graphModule.Path;
const __ = graphTraversalModule.statics;
const t = traversalModule.t;
const P = traversalModule.P;
const direction = traversalModule.direction;

// Determines whether the feature maps (m[]), are deserialized as objects (true) or maps (false).
// Use false for GraphSON3.
const mapAsObject = false;

const parsers = [
  [ 'd\\[(.*)\\]\\.[ilfdm]', toNumeric ],
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
  [ 'c\\[(.+)\\]', toLambda ],
  [ 't\\[(.+)\\]', toT ],
  [ 'D\\[(.+)\\]', toDirection ]
].map(x => [ new RegExp('^' + x[0] + '$'), x[1] ]);

const ignoreReason = {
  setNotSupported: "There is no Set support in gremlin-javascript",
  needsFurtherInvestigation: '',
};

const ignoredScenarios = {
  // An associative array containing the scenario name as key, for example:
  'g_withSideEffectXa_setX_V_both_name_storeXaX_capXaX': new IgnoreError(ignoreReason.setNotSupported),
  'g_withSideEffectXa_setX_V_both_name_aggregateXlocal_aX_capXaX': new IgnoreError(ignoreReason.setNotSupported),
  'g_V_shortestPath_edgesIncluded': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_V_shortestPath_edgesIncluded_edgesXoutEX': new IgnoreError(ignoreReason.needsFurtherInvestigation)
};

Given(/^the (.+) graph$/, function (graphName) {
  if (ignoredScenarios[this.scenario]) {
    return 'skipped';
  }
  this.graphName = graphName;
  const data = this.getData();
  this.g = traversal().withRemote(data.connection);
  if (graphName === 'empty') {
    return this.cleanEmptyGraph();
  }
});

Given('the graph initializer of', function (traversalText) {
  const traversal = vm.runInNewContext(translate(traversalText), getSandbox(this.g, this.parameters));
  return traversal.toList();
});

Given('an unsupported test', () => {});

Given('the traversal of', function (traversalText) {
  this.traversal = vm.runInNewContext(translate(traversalText), getSandbox(this.g, this.parameters));
});

Given(/^using the parameter (.+) of P\.(.+)\("(.+)"\)$/, function (paramName, pval, stringValue) {
  this.parameters[paramName] =  new P(pval, parseValue.call(this, stringValue))
});

Given(/^using the parameter (.+) defined as "(.+)"$/, function (paramName, stringValue) {
  // Remove escaped chars
  stringValue = stringValue.replace(/\\"/g, '"');
  let p = Promise.resolve();
  if (this.graphName === 'empty') {
    p = this.loadEmptyGraphData();
  }
  return p.then(() => {
    this.parameters[paramName] = parseValue.call(this, stringValue);
  }).catch(err => {
    if (err instanceof IgnoreError) {
      return 'skipped';
    }
    throw err;
  });
});

When('iterated to list', function () {
  return this.traversal.toList().then(list => this.result = list);
});

When('iterated next', function () {
  return this.traversal.next().then(it => {
    this.result = it.value;
    if (this.result instanceof Path) {
      // Compare using the objects array
      this.result = this.result.objects;
    }
  });
});

Then(/^the result should be (\w+)$/, function assertResult(characterizedAs, resultTable) {
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
      expect(toCompare(this.result)).to.have.deep.ordered.members(expectedResult);
      break;
    case 'unordered':
      expect(toCompare(this.result)).to.have.deep.members(expectedResult);
      break;
    case 'of':
      // result is a subset of the expected
      expect(expectedResult).to.include.deep.members(toCompare(this.result));
      break;
  }
});

Then(/^the graph should return (\d+) for count of "(.+)"$/, function (stringCount, traversalText) {
  const traversal = vm.runInNewContext(translate(traversalText), getSandbox(this.g, this.parameters));
  return traversal.toList().then(list => {
    expect(list).to.have.lengthOf(parseInt(stringCount, 10));
  });
});

Then(/^the result should have a count of (\d+)$/, function (stringCount) {
  const expected = parseInt(stringCount, 10);
  if (!Array.isArray(this.result)) {
    let count = 0;
    if (this.result instanceof Map) {
      count = this.result.size;
    }
    else if (typeof this.result === 'object') {
      count = Object.keys(this.result).length;
    }
    else {
      throw new Error('result not supported: ' + util.inspect(this.result));
    }
    expect(count).to.be.equal(expected);
    return;
  }
  expect(this.result).to.have.lengthOf(expected);
});

Then('nothing should happen because', _ => {});

function getSandbox(g, parameters) {
  const sandbox = {
    g: g,
    __: __,
    Barrier: traversalModule.barrier,
    Cardinality: traversalModule.cardinality,
    Column: traversalModule.column,
    Direction: {
      BOTH: traversalModule.direction.both,
      IN: traversalModule.direction.in,
      OUT: traversalModule.direction.out
    },
    Order: traversalModule.order,
    P: traversalModule.P,
    TextP: traversalModule.TextP,
    IO: traversalModule.IO,
    Pick: traversalModule.pick,
    Pop: traversalModule.pop,
    Scope: traversalModule.scope,
    Operator: traversalModule.operator,
    T: traversalModule.t,
    toLong: utils.toLong,
    WithOptions: traversalModule.withOptions
  };
  // Pass the parameter to the sandbox
  Object.keys(parameters).forEach(paramName => sandbox[paramName] = parameters[paramName]);
  return sandbox;
}

function translate(traversalText) {
  // clean up trailing period/spaces so that it's easier to match newline with() to convert to with_() below
  traversalText = traversalText.replace(/\)\.\s*/g, ').');
  // Remove escaped chars
  traversalText = traversalText.replace(/\\"/g, '"');
  // Replace long suffix with Long instance
  traversalText = traversalText.replace(/\b(\d+)l\b/gi, 'toLong($1)');
  // Change according to naming convention
  traversalText = traversalText.replace(/\.in\(/g, '.in_(');
  traversalText = traversalText.replace(/\.from\(/g, '.from_(');
  traversalText = traversalText.replace(/\.with\(/g, '.with_(');
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
  return this.getData().vertices.get(name);
}

function toVertexId(name) {
  return toVertex.call(this, name).id;
}

function toVertexIdString(name) {
  return toVertex.call(this, name).id.toString();
}

function toEdge(name) {
  const e = this.getData().edges[name];
  if (!e) {
    throw new Error(util.format('Edge with key "%s" not found', name));
  }
  return e;
}

function toEdgeId(name) {
  return toEdge.call(this, name).id;
}

function toEdgeIdString(name) {
  return toEdge.call(this, name).id.toString();
}

function toPath(value) {
  const parts = value.split(',');
  return new Path(new Array(0), parts.map(x => parseValue.call(this, x)));
}

function toT(value) {
  return t[value];
}

function toDirection(value) {
    return direction[value.toLowerCase()];
}

function toArray(stringList) {
  if (stringList === '') {
    return new Array(0);
  }
  return stringList.split(',').map(x => parseValue.call(this, x));
}

function toMap(stringMap) {
  return parseMapValue.call(this, JSON.parse(stringMap));
}

function parseMapValue(value) {
  if (typeof value === 'string') {
    return parseValue.call(this, value);
  }
  if (Array.isArray(value)) {
    return value.map(x => parseMapValue.call(this, x));
  }
  if (typeof value !== 'object') {
    return value;
  }
  if (mapAsObject) {
    const result = {};
    Object.keys(value).forEach(key => {
      result[parseMapValue.call(this, key)] = parseMapValue.call(this, value[key]);
    });
    return result;
  }
  const map = new Map();
  Object.keys(value).forEach(key => {
    map.set(parseMapValue.call(this, key), parseMapValue.call(this, value[key]));
  });
  return map;
}

function toLambda() {
  throw new IgnoreError(ignoreReason.lambdaNotSupported);
}

/**
 * Adapts the object to be compared, removing labels property of the Path.
 */
function toCompare(obj) {
  if (!obj) {
    return obj;
  }
  if (typeof obj !== 'object') {
    return obj;
  }
  if (Array.isArray(obj)) {
    return obj.map(toCompare);
  }
  if (obj instanceof Path) {
    // labels are ignored
    obj.labels = new Array(0);
  }
  return obj;
}

function IgnoreError(reason) {
  Error.call(this, reason);
  Error.captureStackTrace(this, IgnoreError);
}

util.inherits(IgnoreError, Error);
