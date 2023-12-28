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

function isElement(obj) {
    return obj.hasOwnProperty('id') && obj.hasOwnProperty('label');
}

function deepSort(obj) {
    if (!Array.isArray(obj)) {
        return obj;
    } else {
        obj.map((item) => deepSort(item));
    }
    return obj.sort();
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

function deepMembersByIdOrdered(a, b) {
    return deepMembersById(a, b, true)
}

module.exports = {
    deepCopy,
    deepMembersById,
    deepMembersByIdOrdered,
    deepSort,
    isElement
}