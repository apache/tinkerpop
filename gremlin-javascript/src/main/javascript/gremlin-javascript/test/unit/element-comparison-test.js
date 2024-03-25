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

import chai from 'chai';
import { expect } from 'chai';
import { VertexProperty, Property, Vertex, Edge, Path } from '../../lib/structure/graph.js';
import { deepMembersById, opt } from '../cucumber/element-comparison.js';
import deepEqual from 'deep-eql';

chai.use(function (chai, chaiUtils) {
  chai.Assertion.overwriteMethod('members', function (_super) {
    return deepMembersById;
  });
});

describe('primitives', function () {
    it('should pass', function () {
        expect(deepEqual(1, 1, opt)).to.be.true;
        expect(deepEqual(false, false, opt)).to.be.true;
        expect(deepEqual(null, null, opt)).to.be.true;
    });

    it('should fail', function () {
        expect(deepEqual(1, 2, opt)).to.be.false;
        expect(deepEqual(true, false, opt)).to.be.false;
        expect(deepEqual(0, "0", opt)).to.be.false;
        expect(deepEqual(0, false, opt)).to.be.false;
        expect(deepEqual(0, null, opt)).to.be.false;
        expect(deepEqual(false, null, opt)).to.be.false;
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
        expect(deepEqual(v1, v2, opt)).to.be.true;
        expect(deepEqual(e2, e3, opt)).to.be.true;
        expect(deepEqual(vp1, vp2, opt)).to.be.true;
    });

    it('should fail with different id, same values', function () {
        expect(deepEqual(v2, v3, opt)).to.be.false;
        expect(deepEqual(e1, e2, opt)).to.be.false;
        expect(deepEqual(vp2, vp3, opt)).to.be.false;
    });

    it('should fail with same id, different type', function () {
        expect(deepEqual(v3, e1, opt)).to.be.false;
        expect(deepEqual(e3, vp1, opt)).to.be.false;
    });

    describe('element arrays', function () {
        const ea1 = [v1, e2, v3];
        const ea2 = [v1, e2, v3];
        const ea3 = [e2, v1, v3];
        const ea4 = [e2, e2, v3];
        const ea5 = [v1, e2, v3, vp1];

        it('unordered', function () {
            expect(ea1).to.have.deep.members(ea2);
            expect(ea1).to.have.deep.members(ea3);
            expect(ea1).to.not.have.deep.members(ea4);
            expect(ea1).to.not.have.deep.members(ea5);
            expect(ea5).to.not.have.deep.members(ea1);
        });

        it('ordered', function () {
            expect(ea1).to.have.deep.ordered.members(ea2);
            expect(ea1).to.not.have.deep.ordered.members(ea3);
            expect(ea1).to.not.have.deep.ordered.members(ea4);
            expect(ea1).to.not.have.deep.ordered.members(ea5);
            expect(ea5).to.not.have.deep.ordered.members(ea1);
        });

        it('include', function () {
            expect(ea1).to.include.deep.members(ea2);
            expect(ea1).to.include.deep.members(ea3);
            expect(ea1).to.include.deep.members(ea4);
            expect(ea1).to.not.include.deep.members(ea5);

            expect(ea2).to.include.deep.members(ea1);
            expect(ea3).to.include.deep.members(ea1);
            expect(ea4).to.not.include.deep.members(ea1);
            expect(ea5).to.include.deep.members(ea1);
        });
    });
});

describe('property', function () {
    const p1 = new Property("a", "1");
    const p2 = new Property("a", "1");
    const p3 = new Property("a", 1);
    const p4 = new Property(1, 1);
    const p5 = new Property(1, "a");

    it('should pass only properties that match exactly', function () {
        expect(deepEqual(p1, p2, opt)).to.be.true;
        expect(deepEqual(p1, p3, opt)).to.be.false;
        expect(deepEqual(p1, p4, opt)).to.be.false;
        expect(deepEqual(p3, p5, opt)).to.be.false;
    });
});

describe('arrays', function () {
    const a1 = [1, 2, 3];
    const a2 = [1, 2, 3];
    const a3 = [2, 1, 3];
    const a4 = [2, 2, 3];
    const a5 = [1, 2, 3, 4];

    it('unordered', function () {
        expect(a1).to.have.deep.members(a2);
        expect(a1).to.have.deep.members(a3);
        expect(a1).to.not.have.deep.members(a4);
        expect(a1).to.not.have.deep.members(a5);
        expect(a5).to.not.have.deep.members(a1);
    });

    it('ordered', function () {
        expect(a1).to.have.deep.ordered.members(a2);
        expect(a1).to.not.have.deep.ordered.members(a3);
        expect(a1).to.not.have.deep.ordered.members(a4);
        expect(a1).to.not.have.deep.ordered.members(a5);
        expect(a5).to.not.have.deep.ordered.members(a1);
    });

    const a6 = [1, 2, a1];
    const a7 = [1, a3, 2];
    const a8 = [1, 2, a5];

    const a9 = [1, a1, a1];
    const a10 = [1, a1, a3];

    const a11 = [a9]
    const a12 = [a10]

    it('unordered nested', function () {
        expect(a6).to.have.not.deep.members(a7); // nested arrays ordered differently don't pass as the same item
        expect(a6).to.not.have.deep.members(a8);
        expect(a9).to.not.have.deep.members(a10);
        expect(a11).to.not.have.deep.members(a12);
    });

    it('ordered nested', function () {
        expect(a6).to.have.deep.ordered.members(a6);
        expect(a6).to.not.have.deep.ordered.members(a7);
        expect(a6).to.not.have.deep.ordered.members(a8);
        expect(a9).to.not.have.deep.ordered.members(a10);
        expect(a10).to.have.deep.ordered.members(a10);
        expect(a11).to.have.deep.ordered.members(a11);
        expect(a11).to.not.have.deep.ordered.members(a12);
    });
});

describe('map', function () {
    const m1 = [new Map([
        ['ripple', [new Vertex(5, 'software', undefined)]],
        ['peter', [new Vertex(6, 'person', undefined)]],
        ['vadas', [new Vertex(2, 'person', undefined)]]
    ])];
    const m2 = [new Map([
        ['ripple', [new Vertex(5, 'software', undefined)]],
        ['peter', [new Vertex(6, 'person', undefined)]]
    ])];
    const m3 = [new Map([
        ['ripple', [new Vertex(5, 'software', undefined)]],
        ['vadas', [new Vertex(2, 'person', undefined)]],
        ['peter', [new Vertex(6, 'person', undefined)]]
    ])];
    const m4 = [new Map([
        ['ripple', [new Vertex(5, 'software', undefined)]],
        ['vadas', [new Vertex(6, 'person', undefined)]],
        ['peter', [new Vertex(2, 'person', undefined)]]
    ])];

    it('unordered', function () {
        expect(m1).to.have.deep.members(m1);
        expect(m1).to.not.have.deep.members(m2);
        expect(m1).to.have.deep.members(m3);
        expect(m1).to.not.have.deep.members(m4);
    });

    it('ordered', function () {
        expect(m1).to.have.deep.ordered.members(m1);
        expect(m1).to.not.have.deep.ordered.members(m2);
        expect(m1).to.have.deep.ordered.members(m3); // map entries can be unordered
        expect(m1).to.not.have.deep.ordered.members(m4);
    });
});

describe('objects', function () {
    const obj1 = [{
        k1: "v1",
        k2: "v2",
        k3: { k4: "v4", k5: "v5" }
    }];
    const obj2 = [{
        k1: "v1",
        k2: "v2",
        k3: { k4: "v4", k5: "v5" }
    }];
    const obj3 = [{
        k2: "v2",
        k1: "v1",
        k3: { k5: "v5", k4: "v4" }
    }];
    const obj4 = [{
        k1: "v1",
        k2: "v2",
        k3: { k4: "v4" }
    }];
    const obj5 = [{
        k1: "v1",
        k2: "v2",
        k3: { k4: "v5", k5: "v4" }
    }];

    it('should pass', function () {
        expect(obj1).to.have.deep.members(obj2); // identical
        expect(obj1).to.have.deep.members(obj3); // order swapped
    });

    it('should fail', function () {
        expect(obj1).to.not.have.deep.members(obj4); // missing nested kvp
        expect(obj1).to.not.have.deep.members(obj5); // values swapped
    });

    const obj6 = [{
        k1: "v1",
        k2: "v2",
        k3: [ 1, 2, 3 ]
    }];
    const obj7 = [{
        k3: [ 1, 2, 3 ],
        k1: "v1",
        k2: "v2"
    }];
    const obj8 = [{
        k1: "v1",
        k2: "v2",
        k3: [ 2, 1, 3 ]
    }];

    it('unordered', function () {
        expect(obj6).to.have.deep.members(obj7);
        expect(obj6).to.not.have.deep.members(obj8);
    });

    it('ordered', function () {
        expect(obj6).to.have.deep.ordered.members(obj7); // kvp can be unordered
        expect(obj6).to.not.have.deep.ordered.members(obj8); // array must be ordered
    });

    const path1 = new Path(["path"], [new Vertex(1, 'person', undefined),
                                      new Vertex(2, 'person', undefined)]);
    const path2 = new Path(["path"], [new Vertex(1, 'person', undefined)]);
    const path3 = new Path(["path"], [new Vertex(2, 'person', undefined),
                                      new Vertex(1, 'person', undefined)]);

    it('unordered', function () {
        expect(deepEqual(path1, path1)).to.be.true;
        expect(deepEqual(path1, path2)).to.be.false;
        expect(deepEqual(path1, path3)).to.be.false;
    });

    it('ordered', function () {
        expect(deepEqual(path1, path1)).to.be.true;
        expect(deepEqual(path1, path2)).to.be.false;
        expect(deepEqual(path1, path3)).to.be.false;
    });

});
