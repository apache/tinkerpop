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

const chai = require('chai');
//const { deepEqual, isElement } = require('./element-deep-eql');
const deepEqual = require('deep-eql');

function isElement(obj) {
  return obj.hasOwnProperty('id') && obj.hasOwnProperty('label');
}

function deepSort(obj) {
    if (!Array.isArray(obj)) {
        return obj;
    } else {
        return obj.map((item) => deepSort(item)).sort();
    }
}

function deepCopy(obj) {
    if (typeof obj !== 'object' || obj === null) {
        return obj;
    }
    let copy;
    if (obj instanceof Map) {
        copy = new Map();
        for (const [k, v] of obj.entries()) {
            copy.set(k, deepCopy(v));
        }
        return copy;
    }
    if (Array.isArray(obj)) {
        copy = [];
        for (let i = 0; i < obj.length; i++) {
            copy[i] = deepCopy(obj[i]);
        }
    } else {
        copy = {};
        for (let k in obj) {
            if (obj.hasOwnProperty(k)) {
                copy[k] = deepCopy(obj[k]);
            }
        }
    }
    return copy;
}

function deepMembersById(a, b, ordered=false) {
    a = deepCopy(a);
    b = deepCopy(b);
    console.log(a)
    console.log(b)
    if (typeof a !== 'object' || typeof b !== 'object' || a === null || b === null) {
        return a === b;
    }
    if (typeof a !== typeof b) {
        return false;
    }
    if (isElement(a) || isElement(b)) {
        return a.id === b.id;
    }
    if (a instanceof Map) {
        a = Array.from(a.entries());
        b = Array.from(b.entries());
    }
    if (Array.isArray(a)) {
        if (a.length !== b.length) {
          return false;
        }
        if (!ordered) {
            a = deepSort(a);
            b = deepSort(b);
        }
        for (let i = 0; i < a.length; i++) {
            if (!deepMembersById(a[i], b[i], ordered)) {
                return false;
            }
        }
        return true;
    }
    const keysA = Object.keys(a);
    const keysB = Object.keys(b);
    if (keysA.length !== keysB.length) {
        return false;
    }
    for (let k of keysA) {
        if (!b.hasOwnProperty(k)) {
            return false;
        }
        if (!deepMembersById(a[k], b[k], ordered)) {
            return false;
        }
    }
    return true;
}

function deepMembersById3(a, b) {
    a = deepCopy(a);
    b = deepCopy(b);
    console.log(a)
    console.log(b)
    if (typeof a !== 'object' || typeof b !== 'object' || a === null || b === null) {
        return a === b;
    }
    if (typeof a !== typeof b) {
        return false;
    }
    if (isElement(a) || isElement(b)) {
        return a.id === b.id;
    }
    if (a instanceof Map) {
        a = Array.from(a.entries());
        b = Array.from(b.entries());
    }
    if (Array.isArray(a)) {
        if (a.length !== b.length) {
          return false;
        }
        if (!ordered) {
            a = deepSort(a);
            b = deepSort(b);
        }
        for (let i = 0; i < a.length; i++) {
            if (!deepMembersById(a[i], b[i], ordered)) {
                return false;
            }
        }
        return true;
    }
    const keysA = Object.keys(a);
    const keysB = Object.keys(b);
    if (keysA.length !== keysB.length) {
        return false;
    }
    for (let k of keysA) {
        if (!b.hasOwnProperty(k)) {
            return false;
        }
        if (!deepMembersById(a[k], b[k], ordered)) {
            return false;
        }
    }
    return true;
}

function deepMembersByIdOrdered(a, b) {
    return deepMembersById(a, b, true)
}

function deepIncludesById(array, subset) {
    console.log(array);
    console.log(subset);
    for (let item of subset) {
        const included = array.some((arrayItem) => deepMembersById(item, arrayItem));
        if (!included) {
            return false;
        }
    }
    return true;
}

function isSubsetOf(subset, superset, cmp, contains, ordered) {
    if (!contains) {
      if (subset.length !== superset.length) return false;
      superset = superset.slice();
    }

    return subset.every(function(elem, idx) {
      if (ordered) return cmp ? cmp(elem, superset[idx], compareElements) : elem === superset[idx];

      if (!cmp) {
        var matchIdx = superset.indexOf(elem);
        if (matchIdx === -1) return false;

        // Remove match from superset so not counted twice if duplicate in subset.
        if (!contains) superset.splice(matchIdx, 1);
        return true;
      }

      return superset.some(function(elem2, matchIdx) {
        if (!cmp(elem, elem2, compareElements)) return false;

        // Remove match from superset so not counted twice if duplicate in subset.
        if (!contains) superset.splice(matchIdx, 1);
        return true;
      });
    });
  }

function flag(obj, key, value) {
  var flags = obj.__flags || (obj.__flags = Object.create(null));
  if (arguments.length === 3) {
    flags[key] = value;
  } else {
    return flags[key];
  }
};

function deepMembersById2 (subset, msg) {
        if (msg) flag(this, 'message', msg);
        var obj = flag(this, 'object')
          , flagMsg = flag(this, 'message')
          , ssfi = flag(this, 'ssfi');

        new chai.Assertion(obj, flagMsg, ssfi, true).to.be.an('array');
        new chai.Assertion(subset, flagMsg, ssfi, true).to.be.an('array');

        var contains = flag(this, 'contains');
        var ordered = flag(this, 'ordered');

        var subject, failMsg, failNegateMsg;

        if (contains) {
          subject = ordered ? 'an ordered superset' : 'a superset';
          failMsg = 'expected #{this} to be ' + subject + ' of #{exp}';
          failNegateMsg = 'expected #{this} to not be ' + subject + ' of #{exp}';
        } else {
          subject = ordered ? 'ordered members' : 'members';
          failMsg = 'expected #{this} to have the same ' + subject + ' as #{exp}';
          failNegateMsg = 'expected #{this} to not have the same ' + subject + ' as #{exp}';
        }

        var cmp = flag(this, 'deep') ? deepEqual : undefined;

        this.assert(
            isSubsetOf(subset, obj, cmp, contains, ordered)
          , failMsg
          , failNegateMsg
          , subset
          , obj
          , true
        );
      }

function compareElements(a, b) {
    if (!isElement(a) || !isElement(b)) {
        return null;
    } else {
        return a.id === b.id;
    }
}


module.exports = {
    deepIncludesById,
    deepMembersById,
    isElement,
    deepMembersById2,
    deepCopy,
    deepSort,
    deepMembersByIdOrdered,
    compareElements
}