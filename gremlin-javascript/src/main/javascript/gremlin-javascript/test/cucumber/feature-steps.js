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
const util = require('util');
const graphModule = require('../../lib/structure/graph');
const graphTraversalModule = require('../../lib/process/graph-traversal');
const traversalModule = require('../../lib/process/traversal');
const utils = require('../../lib/utils');
const traversal = require('../../lib/process/anonymous-traversal').traversal;
const Path = graphModule.Path;
const __ = graphTraversalModule.statics;
const t = traversalModule.t;

// Determines whether the feature maps (m[]), are deserialized as objects (true) or maps (false).
// Use false for GraphSON3.
const mapAsObject = false;

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
  [ 'c\\[(.+)\\]', toLambda ],
  [ 't\\[(.+)\\]', toT ]
].map(x => [ new RegExp('^' + x[0] + '$'), x[1] ]);

const ignoreReason = {
  lambdaNotSupported: 'Lambdas are not supported on gremlin-javascript',
  computerNotSupported: "withComputer() is not supported on gremlin-javascript",
  setNotSupported: "There is no Set support in gremlin-javascript",
  needsFurtherInvestigation: '',
};

const ignoredScenarios = {
  // An associative array containing the scenario name as key, for example:
  'g_V_connectedComponent_hasXcomponentX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_dedup_connectedComponent_hasXcomponentX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_hasLabelXsoftwareX_connectedComponent_project_byXnameX_byXcomponentX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_connectedComponent_withXEDGES_bothEXknowsXX_withXPROPERTY_NAME_clusterX_project_byXnameX_byXclusterX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_pageRank_hasXpageRankX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_outXcreatedX_pageRank_byXbothEX_byXprojectRankX_timesX0X_valueMapXname_projectRankX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_pageRank_order_byXpageRank_decrX_byXnameX_name': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_pageRank_order_byXpageRank_decrX_name_limitX2X': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_pageRank_byXoutEXknowsXX_byXfriendRankX_project_byXnameX_byXvaluesXfriendRankX_mathX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_hasLabelXpersonX_pageRank_byXpageRankX_project_byXnameX_byXvaluesXpageRankX_mathX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_pageRank_byXpageRankX_asXaX_outXknowsX_pageRank_asXbX_selectXa_bX_by_byXmathX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_hasLabelXsoftwareX_hasXname_rippleX_pageRankX1X_byXinEXcreatedXX_timesX1X_byXpriorsX_inXcreatedX_unionXboth__identityX_valueMapXname_priorsX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_outXcreatedX_groupXmX_byXlabelX_pageRankX1X_byXpageRankX_byXinEX_timesX1X_inXcreatedX_groupXmX_byXpageRankX_capXmX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_peerPressure_hasXclusterX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_peerPressure_byXclusterX_byXoutEXknowsXX_pageRankX1X_byXrankX_byXoutEXknowsXX_timesX2X_group_byXclusterX_byXrank_sumX_limitX100X': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_hasXname_rippleX_inXcreatedX_peerPressure_byXoutEX_byXclusterX_repeatXunionXidentity__bothX_timesX2X_dedup_valueMapXname_clusterX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_hasXname_rippleX_inXcreatedX_peerPressure_withXEDGES_outEX_withXPROPERTY_NAME_clusterX_repeatXunionXidentity__bothX_timesX2X_dedup_valueMapXname_clusterX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_shortestPath': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_both_dedup_shortestPath': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_shortestPath_edgesIncluded': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_shortestPath_directionXINX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_shortestPath_edgesXoutEX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_shortestPath_edgesIncluded_edgesXoutEX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_hasXname_markoX_shortestPath': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_shortestPath_targetXhasXname_markoXX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_shortestPath_targetXvaluesXnameX_isXmarkoXX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_hasXname_markoX_shortestPath_targetXhasLabelXsoftwareXX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_hasXname_markoX_shortestPath_targetXhasXname_joshXX_distanceXweightX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_hasXname_danielX_shortestPath_targetXhasXname_stephenXX_edgesXbothEXusesXX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_hasXsong_name_MIGHT_AS_WELLX_shortestPath_targetXhasXsong_name_MAYBE_YOU_KNOW_HOW_I_FEELXX_edgesXoutEXfollowedByXX_distanceXweightX': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_hasXname_markoX_shortestPath_maxDistanceX1X': new IgnoreError(ignoreReason.computerNotSupported),
  'g_V_hasXname_vadasX_shortestPath_distanceXweightX_maxDistanceX1_3X': new IgnoreError(ignoreReason.computerNotSupported),
  'g_withSideEffectXa_setX_V_both_name_storeXaX_capXaX': new IgnoreError(ignoreReason.setNotSupported)
};

defineSupportCode(function(methods) {
  methods.Given(/^the (.+) graph$/, function (graphName) {
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

  methods.Given('the graph initializer of', function (traversalText) {
    const traversal = vm.runInNewContext(translate(traversalText), getSandbox(this.g, this.parameters));
    return traversal.toList();
  });

  methods.Given('an unsupported test', () => {});

  methods.Given('the traversal of', function (traversalText) {
    this.traversal = vm.runInNewContext(translate(traversalText), getSandbox(this.g, this.parameters));
  });

  methods.Given(/^using the parameter (.+) defined as "(.+)"$/, function (paramName, stringValue) {
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

  methods.When('iterated to list', function () {
    return this.traversal.toList().then(list => this.result = list);
  });

  methods.When('iterated next', function () {
    return this.traversal.next().then(it => {
      this.result = it.value;
      if (this.result instanceof Path) {
        // Compare using the objects array
        this.result = this.result.objects;
      }
    });
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

  methods.Then(/^the graph should return (\d+) for count of "(.+)"$/, function (stringCount, traversalText) {
    const traversal = vm.runInNewContext(translate(traversalText), getSandbox(this.g, this.parameters));
    return traversal.toList().then(list => {
      expect(list).to.have.lengthOf(parseInt(stringCount, 10));
    });
  });

  methods.Then(/^the result should have a count of (\d+)$/, function (stringCount) {
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

  methods.Then('nothing should happen because', _ => {});
});

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
