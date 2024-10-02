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

const {Given, Then, When, setDefaultTimeout} = require('cucumber');
// Setting Cucumber timeout to 10s for Floating Errors on Windows on GitHub Actions
setDefaultTimeout(10 * 1000);
const chai = require('chai')
chai.use(require('chai-string'));
const expect = chai.expect;
const util = require('util');
const gremlin = require('./gremlin').gremlin;
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
const merge = traversalModule.merge;
const deepMembersById = require('./element-comparison').deepMembersById;
const parsers = [
  [ 'str\\[(.*)\\]', (stringValue) => stringValue ], //returns the string value as is
  [ 'vp\\[(.+)\\]', toVertexProperty ],
  [ 'dt\\[(.+)\\]', toDateTime ],
  [ 'd\\[(.*)\\]\\.[bsilfdmn]', toNumeric ],
  [ 'v\\[(.+)\\]', toVertex ],
  [ 'v\\[(.+)\\]\\.id', toVertexId ],
  [ 'v\\[(.+)\\]\\.sid', toVertexIdString ],
  [ 'e\\[(.+)\\]', toEdge ],
  [ 'e\\[(.+)\\]\\.id', toEdgeId ],
  [ 'e\\[(.+)\\]\\.sid', toEdgeIdString ],
  [ 'vp\\[(.+)\\]', toVertexProperty ],
  [ 'p\\[(.+)\\]', toPath ],
  [ 'l\\[(.*)\\]', toArray ],
  [ 's\\[(.*)\\]', toSet ],
  [ 'm\\[(.+)\\]', toMap ],
  [ 'c\\[(.+)\\]', toLambda ],
  [ 't\\[(.+)\\]', toT ],
  [ 'D\\[(.+)\\]', toDirection ],
  [ 'M\\[(.+)\\]', toMerge ]
].map(x => [ new RegExp('^' + x[0] + '$'), x[1] ]);

chai.use(function (chai, chaiUtils) {
  chai.Assertion.overwriteMethod('members', function (_super) {
    return deepMembersById;
  });
});

const ignoreReason = {
  classNotSupported: "Javascript does not support the class type in GraphBinary",
  nullKeysInMapNotSupportedWell: "Javascript does not nicely support 'null' as a key in Map instances",
  needsFurtherInvestigation: '',
};

const ignoredScenarios = {
  // An associative array containing the scenario name as key, for example:
  'g_withStrategiesXProductiveByStrategyX_V_groupCount_byXageX': new IgnoreError(ignoreReason.nullKeysInMapNotSupportedWell),
  'g_withoutStrategiesXCountStrategyX_V_count': new IgnoreError(ignoreReason.classNotSupported),
  'g_V_shortestPath_edgesIncluded': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_V_shortestPath_edgesIncluded_edgesXoutEX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_V_shortestpath': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  // after backport of strategy construction improvements from master, there are now test failures (not currently running GLV tests on master)
  'g_withStrategiesXAdjacentToIncidentStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXAdjacentToIncidentStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXAdjacentToIncidentStrategyX_V_out_count': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXAdjacentToIncidentStrategyX_V_whereXoutX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXByModulatorOptimizationStrategyX_V_order_byXvaluesXnameXX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXByModulatorOptimizationStrategyX_V_order_byXvaluesXnameXX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXComputerFinalizationStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXByModulatorOptimizationStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXComputerVerificationStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXComputerVerificationStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXConnectiveStrategyStrategyX_V_hasXname_markoX_or_whereXinXknowsX_hasXname_markoXX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXConnectiveStrategyX_V_hasXname_markoX_or_whereXinXknowsX_hasXname_markoXX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXCountStrategyX_V_whereXoutE_count_isX0XX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXCountStrategyX_V_whereXoutE_count_isX0XX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXEarlyLimitStrategyX_V_out_order_valueMap_limitX3X_selectXnameX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXEarlyLimitStrategyX_V_out_order_valueMap_limitX3X_selectXnameX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXEdgeLabelVerificationStrategyXthrowException_true_logWarning_falseXX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXEdgeLabelVerificationStrategyXthrowException_false_logWarning_falseXX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXEdgeLabelVerificationStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXElementIdStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXElementIdStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXFilterRankingStrategyX_V_out_order_dedup': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXFilterRankingStrategyX_V_out_order_dedup': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXGraphFilterStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXGraphFilterStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXHaltedTraverserStrategyXDetachedFactoryXX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXHaltedTraverserStrategyXReferenceFactoryXX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXHaltedTraverserStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXIdentityRemovalStrategyX_V_identity_out': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXIdentityRemovalStrategyX_V_identity_out': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXIncidentToAdjacentStrategyX_V_outE_inV': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXIncidentToAdjacentStrategyX_V_outE_inV': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXInlineFilterStrategyX_V_filterXhasXname_markoXX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXInlineFilterStrategyX_V_filterXhasXname_markoXX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXLazyBarrierStrategyX_V_out_bothE_count': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXLazyBarrierStrategyX_V_out_bothE_count': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXMatchAlgorithmStrategyXmatchAlgorithm_CountMatchAlgorithmXX_V_matchXa_knows_b__a_created_cX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXMatchAlgorithmStrategyXmatchAlgorithm_GreedyMatchAlgorithmXX_V_matchXa_knows_b__a_created_cX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXMatchAlgorithmStrategyX_V_matchXa_knows_b__a_created_cX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXMatchPredicateStrategyX_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXa_b_cX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXMatchPredicateStrategyX_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXa_b_cX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXMessagePassingReductionStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXMessagePassingReductionStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXOptionsStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXOrderLimitStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXOrderLimitStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXPathProcessorStrategyX_V_asXaX_selectXaX_byXvaluesXnameXX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXPathProcessorStrategyX_V_asXaX_selectXaX_byXvaluesXnameXX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXPathRetractionStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXPathRetractionStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXProductiveByStrategyX_V_group_byXageX_byXnameX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXProfileStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXProfileStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXReferenceElementStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXReferenceElementStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXRepeatUnrollStrategyX_V_repeatXoutX_timesX2X': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXRepeatUnrollStrategyX_V_repeatXoutX_timesX2X': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXReservedKeysVerificationStrategyXthrowException_trueXX_addVXpersonX_propertyXid_123X_propertyXname_markoX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXReservedKeysVerificationStrategyXthrowException_trueXX_addVXpersonX_propertyXage_29X_propertyXname_markoX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXReservedKeysVerificationStrategyX_addVXpersonX_propertyXid_123X_propertyXname_markoX': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXSeedStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXStandardVerificationStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXStandardVerificationStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXVertexProgramRestrictionStrategyX_withoutStrategiesXVertexProgramStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withStrategiesXVertexProgramRestrictionStrategy_VertexProgramStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXVertexProgramRestrictionStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
  'g_withoutStrategiesXVertexProgramStrategyX_V': new IgnoreError(ignoreReason.needsFurtherInvestigation),
};

Given(/^the (.+) graph$/, function (graphName) {
  // if the scenario is ignored or if the scenario has no gremlin (i.e. happens for skipped lambdas that can't
  // translate) then skipp the test
  if (ignoredScenarios[this.scenario] || gremlin[this.scenario].length === 0) {
    return 'skipped';
  }
  this.graphName = graphName;
  const data = this.getData();
  this.g = traversal().with_(data.connection);

  if (this.isGraphComputer) {
    this.g = this.g.withComputer();
  }

  if (graphName === 'empty') {
    return this.cleanEmptyGraph();
  }
});

Given('the graph initializer of', function (traversalText) {
  const p = Object.assign({}, this.parameters);
  p.g = this.g;
  const traversal = gremlin[this.scenario].shift()(p);
  return traversal.toList();
});

Given('an unsupported test', () => {});

Given('the traversal of', function (traversalText) {
  const p = Object.assign({}, this.parameters);
  p.g = this.g;
  this.traversal = gremlin[this.scenario].shift()(p);
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

var removeProperties = function(p) {
  if (p === undefined) {   
  } else if (p instanceof graphModule.Vertex || p instanceof graphModule.Edge) {
    p.properties = undefined;
  } else if (p instanceof Array) {
    p.forEach(removeProperties)
  } else if (p instanceof Map) {
    removeProperties(Array.from(p.keys()))
    removeProperties(Array.from(p.values()))
  } else if (p instanceof graphModule.Path) {
    removeProperties(p.objects)
  }

  return p
}

When('iterated to list', function () {
  return this.traversal.toList().then(list => this.result = removeProperties(list)).catch(err => this.result = err);
});

When('iterated next', function () {
  return this.traversal.next().then(it => {
    // for Path compare using the objects array
    this.result = removeProperties(it.value instanceof Path ? it.value.objects : it.value )
  }).catch(err => this.result = err);
});

Then('the traversal will raise an error', function() {
  expect(this.result).to.be.a.instanceof(Error);
});

Then(/^the traversal will raise an error with message (\w+) text of "(.+)"$/, function(comparison, expectedMessage) {
  expect(this.result).to.be.a.instanceof(Error);
  if (comparison === "containing") {
    expect(this.result.message).to.contain(expectedMessage)
  } else if (comparison === "starting") {
    expect(this.result.message).to.startWith(expectedMessage)
  } else if (comparison === "ending") {
    expect(this.result.message).to.endWith(expectedMessage)
  } else {
    throw new Error('unknown comparison \'' + comparison + '\'- must be: containing, ending or starting');
  }
});

Then(/^the result should be (\w+)$/, function assertResult(characterizedAs, resultTable) {
  expect(this.result).to.not.be.a.instanceof(Error);

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
  expect(this.result).to.not.be.a.instanceof(Error);

  const p = Object.assign({}, this.parameters);
  p.g = this.g;
  const traversal = gremlin[this.scenario].shift()(p);
  return traversal.toList().then(list => {
    expect(list).to.have.lengthOf(parseInt(stringCount, 10));
  });
});

Then(/^the result should have a count of (\d+)$/, function (stringCount) {
  expect(this.result).to.not.be.a.instanceof(Error);

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
      OUT: traversalModule.direction.out,
      from_: traversalModule.direction.in,
      to: traversalModule.direction.out,
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

function parseRow(row) {
  return parseValue.call(this, row[0]);
}

function parseValue(stringValue) {

  if(stringValue === "null")
    return null;
  if(stringValue === "true")
    return true;
  if(stringValue === "false")
    return false;
  if(stringValue === "d[NaN]")
    return Number.NaN;
  if(stringValue === "d[Infinity]")
    return Number.POSITIVE_INFINITY;
  if(stringValue === "d[-Infinity]")
    return Number.NEGATIVE_INFINITY;

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
  try {
    return parseFloat(stringValue);
  } catch (Error) {
    return BigInt(stringValue);
  }
}

function toVertex(name) {
  // some vertices are cached, like those from toy graphs but some are just references. if they are
  // not cached then they are meant to be references.
  const vertices = this.getData().vertices;
  if (vertices.has(name))
    return this.getData().vertices.get(name);
  else
    return new graphModule.Vertex(name, "vertex")
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

function toVertexProperty(name) {
  const vp = this.getData().vertexProperties[name];
  if (!vp) {
    throw new Error(util.format('VertexProperty with key "%s" not found', name));
  }
  return vp;
}

function toPath(value) {
  const parts = value.split(',');
  return new Path(new Array(0), parts.map(x => parseValue.call(this, x)));
}

function toT(value) {
  return t[value];
}

function toDirection(value) {
  // swap Direction.from alias
  if (value === 'from')
    return direction["from_"];
  else
    return direction[value.toLowerCase()];
}

function toDateTime(value) {
  return new Date(value);
}

function toMerge(value) {
  return merge[value];
}

function toArray(stringList) {
  if (stringList === '') {
    return new Array(0);
  }
  return stringList.split(',').map(x => parseValue.call(this, x));
}

function toSet(stringList) {
  if (stringList === '') {
    return new Set();
  }

  const s = new Set();
  stringList.split(',').forEach(x => s.add(parseValue.call(this, x)));
  return s;
}

function toMap(stringMap) {
  return parseMapValue.call(this, JSON.parse(stringMap));
}

function parseMapValue(value) {
  if (value === null)
    return null;

  if (typeof value === 'string') {
    return parseValue.call(this, value);
  }
  if (Array.isArray(value)) {
    return value.map(x => parseMapValue.call(this, x));
  }
  if (typeof value !== 'object') {
    return value;
  }
  const map = new Map();
  Object.keys(value).forEach(key => {
    map.set(parseMapValue.call(this, key), parseMapValue.call(this, value[key]));
  });
  return map;
}

function toLambda(stringLambda) {
  return () => [stringLambda, "gremlin-groovy"];
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
