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

/*
 * Portions of this code are based on the Chai Assertion Library
 * at https://www.chaijs.com/, which is licensed under the MIT License.
 * The functions deepMembersById, flag, and isSubsetOf are adapted from
 * Chai's source code.
 * See licenses/chai for full license.
 */

import { Assertion } from 'chai';
import deepEqual from 'deep-eql';
import { Edge, Vertex, VertexProperty } from '../../lib/structure/graph.js';

function isElement(obj) {
    return obj instanceof Edge || obj instanceof Vertex || obj instanceof VertexProperty;
}

export const opt = {comparator: compareElements};

function isSubsetOf(subset, superset, cmp, contains, ordered) {
    if (!contains) {
        if (subset.length !== superset.length) return false;
        superset = superset.slice();
    }

    return subset.every(function(elem, idx) {
        if (ordered) return cmp ? cmp(elem, superset[idx], opt) : elem === superset[idx];

        if (!cmp) {
            var matchIdx = superset.indexOf(elem);
            if (matchIdx === -1) return false;

            // Remove match from superset so not counted twice if duplicate in subset.
            if (!contains) superset.splice(matchIdx, 1);
            return true;
        }

        return superset.some(function(elem2, matchIdx) {
            if (!cmp(elem, elem2, opt)) return false;

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

export function deepMembersById (subset, msg) {
    if (msg) flag(this, 'message', msg);
    var obj = flag(this, 'object')
        , flagMsg = flag(this, 'message')
        , ssfi = flag(this, 'ssfi');

    new Assertion(obj, flagMsg, ssfi, true).to.be.an('array');
    new Assertion(subset, flagMsg, ssfi, true).to.be.an('array');

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

export function compareElements(a, b) {
    if (!isElement(a) || !isElement(b)) {
        return null;
    } else {
        return a.constructor === b.constructor && a.id === b.id;
    }
}
