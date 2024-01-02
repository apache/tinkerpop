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

const { expect } = require('chai');
const { VertexProperty, Property, Vertex, Edge, Path } = require('../../lib/structure/graph');
const { deepCopy, deepMembersById, deepMembersByIdOrdered, deepSort, isElement} = require('../cucumber/element-comparison');


describe('primitives', function () {
    it('should pass', function () {
        expect(deepMembersById(1, 1)).to.be.true;
        expect(deepMembersById(false, false)).to.be.true;
        expect(deepMembersById(null, null)).to.be.true;
    });

    it('should fail', function () {
        expect(deepMembersById(1, 2)).to.be.false;
        expect(deepMembersById(true, false)).to.be.false;
        expect(deepMembersById(0, "0")).to.be.false;
        expect(deepMembersById(0, false)).to.be.false;
        expect(deepMembersById(0, null)).to.be.false;
        expect(deepMembersById(false, null)).to.be.false;
    });
});

describe('elements', function () {
    const v1 = new Vertex(1, "dog", undefined);
    const v2 = new Vertex(1, "cat", undefined);
    const v3 = new Vertex(2, "cat", undefined);

    const e1 = new Edge(2, v1, "chases", v3, undefined);
    const e2 = new Edge(3, v1, "chases", v3, undefined);
    const e3 = new Edge(3, v2, "chases", v3, undefined);

    const vp1 = new VertexProperty(3, "size", "small", undefined);
    const vp2 = new VertexProperty(3, "size", "large", undefined);
    const vp3 = new VertexProperty(4, "size", "large", undefined);

    it('should pass with same id, different values', function () {
        expect(deepMembersById(v1, v2)).to.be.true;
        expect(deepMembersById(v3, e1)).to.be.true;
        expect(deepMembersById(e2, e3)).to.be.true;
        expect(deepMembersById(e3, vp1)).to.be.true;
        expect(deepMembersById(vp1, vp2)).to.be.true;
    });

    it('should fail with different id, same values', function () {
        expect(deepMembersById(v2, v3)).to.be.false;
        expect(deepMembersById(e1, e2)).to.be.false;
        expect(deepMembersById(vp2, vp3)).to.be.false;
    });
});

describe('property', function () {
    const p1 = new Property("a", "1");
    const p2 = new Property("a", "1");
    const p3 = new Property("a", 1);
    const p4 = new Property(1, 1);
    const p5 = new Property(1, "a");

    it('should pass only properties that match exactly', function () {
        expect(deepMembersById(p1, p2)).to.be.true;
        expect(deepMembersById(p1, p3)).to.be.false;
        expect(deepMembersById(p1, p4)).to.be.false;
        expect(deepMembersById(p3, p5)).to.be.false;
        expect(deepMembersById(p3, p5)).to.be.false;
    });
});

describe('arrays', function () {
    const a1 = [1, 2, 3];
    const a2 = [1, 2, 3];
    const a3 = [2, 1, 3];
    const a4 = [2, 2, 3];
    const a5 = [1, 2, 3, 4];

    it('unordered', function () {
        expect(deepMembersById(a1, a2)).to.be.true;
        expect(deepMembersById(a1, a3)).to.be.true;
        expect(deepMembersById(a1, a4)).to.be.false;
        expect(deepMembersById(a1, a5)).to.be.false;
        expect(deepMembersById(a5, a1)).to.be.false;
    });

    it('ordered', function () {
        expect(deepMembersByIdOrdered(a1, a2)).to.be.true;
        expect(deepMembersByIdOrdered(a1, a3)).to.be.false;
        expect(deepMembersByIdOrdered(a1, a4)).to.be.false;
        expect(deepMembersByIdOrdered(a1, a5)).to.be.false;
        expect(deepMembersByIdOrdered(a5, a1)).to.be.false;
    });

    const a6 = [1, 2, a1];
    const a7 = [1, a3, 2];
    const a8 = [1, 2, a5];

    const a9 = [1, a1, a1];
    const a10 = [1, a1, a3];

    const a11 = [a9]
    const a12 = [a10]

    it('unordered nested', function () {
        expect(deepMembersById(a6, a7)).to.be.true;
        expect(deepMembersById(a6, a8)).to.be.false;
        expect(deepMembersById(a9, a10)).to.be.true;
        expect(deepMembersById(a11, a12)).to.be.true;
    });

    it('ordered nested', function () {
        expect(deepMembersByIdOrdered(a6, a6)).to.be.true;
        expect(deepMembersByIdOrdered(a6, a7)).to.be.false;
        expect(deepMembersByIdOrdered(a6, a8)).to.be.false;
        expect(deepMembersByIdOrdered(a9, a10)).to.be.false;
        expect(deepMembersByIdOrdered(a10, a10)).to.be.true;
        expect(deepMembersByIdOrdered(a11, a11)).to.be.true;
        expect(deepMembersByIdOrdered(a11, a12)).to.be.false;
    });
});

describe('map', function () {
    const m1 = new Map([
        ['ripple', [new Vertex(5, 'software', undefined)]],
        ['peter', [new Vertex(6, 'person', undefined)]],
        ['vadas', [new Vertex(2, 'person', undefined)]]
    ]);
    const m2 = new Map([
        ['ripple', [new Vertex(5, 'software', undefined)]],
        ['peter', [new Vertex(6, 'person', undefined)]]
    ]);
    const m3 = new Map([
        ['ripple', [new Vertex(5, 'software', undefined)]],
        ['vadas', [new Vertex(2, 'person', undefined)]],
        ['peter', [new Vertex(6, 'person', undefined)]]
    ]);
    const m4 = new Map([
        ['ripple', [new Vertex(5, 'software', undefined)]],
        ['vadas', [new Vertex(6, 'person', undefined)]],
        ['peter', [new Vertex(2, 'person', undefined)]]
    ]);

    it('unordered', function () {
        expect(deepMembersById(m1, m1)).to.be.true;
        expect(deepMembersById(m1, m2)).to.be.false;
        expect(deepMembersById(m1, m3)).to.be.true;
        expect(deepMembersById(m1, m4)).to.be.false;
    });

    it('ordered', function () {
        expect(deepMembersByIdOrdered(m1, m1)).to.be.true;
        expect(deepMembersByIdOrdered(m1, m2)).to.be.false;
        expect(deepMembersByIdOrdered(m1, m3)).to.be.false;
        expect(deepMembersByIdOrdered(m1, m4)).to.be.false;
    });
});

describe('objects', function () {
    const obj1 = {
        k1: "v1",
        k2: "v2",
        k3: { k4: "v4", k5: "v5" }
    };
    const obj2 = {
        k1: "v1",
        k2: "v2",
        k3: { k4: "v4", k5: "v5" }
    };
    const obj3 = {
        k2: "v2",
        k1: "v1",
        k3: { k5: "v5", k4: "v4" }
    };
    const obj4 = {
        k1: "v1",
        k2: "v2",
        k3: { k4: "v4" }
    };
    const obj5 = {
        k1: "v1",
        k2: "v2",
        k3: { k4: "v5", k5: "v4" }
    };

    it('should pass', function () {
        expect(deepMembersById(obj1, obj2)).to.be.true; // identical
        expect(deepMembersById(obj1, obj3)).to.be.true; // order swapped
    });

    it('should fail', function () {
        expect(deepMembersById(obj1, obj4)).to.be.false; // missing nested kvp
        expect(deepMembersById(obj1, obj5)).to.be.false; // values swapped
    });

    const obj6 = {
        k1: "v1",
        k2: "v2",
        k3: [ 1, 2, 3 ]
    };
    const obj7 = {
        k3: [ 1, 2, 3 ],
        k1: "v1",
        k2: "v2"
    };
    const obj8 = {
        k1: "v1",
        k2: "v2",
        k3: [ 2, 1, 3 ]
    }

    it('unordered', function () {
        expect(deepMembersById(obj6, obj7)).to.be.true;
        expect(deepMembersById(obj6, obj8)).to.be.true;
    });

    it('ordered', function () {
        expect(deepMembersByIdOrdered(obj6, obj7)).to.be.true; // kvp can be unordered
        expect(deepMembersByIdOrdered(obj6, obj8)).to.be.false; // array must be ordered
    });

    const path1 = new Path(["path"], [new Vertex(1, 'person', undefined),
                                      new Vertex(2, 'person', undefined)]);
    const path2 = new Path(["path"], [new Vertex(1, 'person', undefined)]);
    const path3 = new Path(["path"], [new Vertex(2, 'person', undefined),
                                      new Vertex(1, 'person', undefined)]);

    it('unordered', function () {
        expect(deepMembersById(path1, path1)).to.be.true;
        expect(deepMembersById(path1, path2)).to.be.false;
        expect(deepMembersById(path1, path3)).to.be.false;
    });

    it('ordered', function () {
        expect(deepMembersById(path1, path1)).to.be.true;
        expect(deepMembersById(path1, path2)).to.be.false;
        expect(deepMembersById(path1, path3)).to.be.false;
    });

});
