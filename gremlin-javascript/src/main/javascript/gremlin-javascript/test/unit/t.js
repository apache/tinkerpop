///*
// *  Licensed to the Apache Software Foundation (ASF) under one
// *  or more contributor license agreements.  See the NOTICE file
// *  distributed with this work for additional information
// *  regarding copyright ownership.  The ASF licenses this file
// *  to you under the Apache License, Version 2.0 (the
// *  "License"); you may not use this file except in compliance
// *  with the License.  You may obtain a copy of the License at
// *
// *  http://www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing,
// *  software distributed under the License is distributed on an
// *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// *  KIND, either express or implied.  See the License for the
// *  specific language governing permissions and limitations
// *  under the License.
// */
//'use strict';
//
//const util = require('util');
//const gremlin = require('../cucumber/gremlin').gremlin;
//const utils = require('../../lib/utils');
//const traversal = require('../../lib/process/anonymous-traversal').traversal;
//const { VertexProperty, Property, Vertex, Edge, Path } = require('../../lib/structure/graph');
//const { deepCopy, deepMembersById, deepMembersByIdOrdered, deepSort, isElement} = require('../cucumber/element-comparison')
//
////const m1 = new Map([
////  ['ripple', [new Vertex(5, 'software', undefined)]],
////  ['peter', [new Vertex(6, 'person', undefined)]],
////  ['vadas', [new Vertex(2, 'person', undefined)]],
////  ['josh', [new Vertex(4, 'person', undefined)]],
////  ['lop', [new Vertex(3, 'software', undefined)]],
////  ['marko', [new Vertex(1, 'person', undefined)]]
////]);
////
////const m2 = new Map([
////  ['ripple', [new Vertex(5, 'software', undefined)]],
////  ['peter', [new Vertex(6, 'person', undefined)]],
////  ['vadas', [new Vertex(2, 'person', undefined)]],
////  ['josh', [new Vertex(4, 'person', undefined)]],
////  ['lop', [new Vertex(3, 'software', undefined)]],
////]);
////
////const m3 = new Map([
////  ['ripple', [new Vertex(5, 'software', undefined)]],
////  ['peter', [new Vertex(6, 'person', undefined)]],
////  ['vadas', [new Vertex(2, 'person', undefined)]],
////  ['lop', [new Vertex(3, 'software', undefined)]],
////  ['josh', [new Vertex(4, 'person', undefined)]],
////  ['marko', [new Vertex(1, 'person', undefined)]]
////]);
//
//const a1 = [
//  new Vertex(5, 'software', undefined),
//  new Vertex(6, 'person', undefined),
//  new Vertex(2, 'person', undefined),
//  new Vertex(4, 'person', undefined),
//  new Vertex(3, 'software', undefined),
//  new Vertex(1, 'person', undefined)
//];
//
//const a2 = [
//  new Vertex(5, 'software', undefined),
//  new Vertex(6, 'person', undefined),
//  new Vertex(2, 'person', undefined),
//  new Vertex(4, 'person', undefined),
//  new Vertex(3, 'software', undefined)
//];
//
//const a3 = [
//  new Vertex(5, 'software', undefined),
//  new Vertex(6, 'person', undefined),
//  new Vertex(2, 'person', undefined),
//  new Vertex(3, 'software', undefined),
//  new Vertex(4, 'person', undefined),
//  new Vertex(1, 'person', undefined)
//];
//
//const v1 = new Vertex(8, "dog", undefined)
//const v2 = new Vertex(9, "cat", undefined)
//
//const pl1 = [
//  new Path([], [ 'daniel', 6 ]),
//  new Path([], [ v1, v2, 'lop', 3 ]),
//  new Path([], [ v1, v2, 'vadas', 5 ]),
//  new Path([], [ v1, v2, 'josh', 4 ])
//]
//
//const pl2 = [
//  new Path([], [ 'daniel', 6 ]),
//  new Path([], [ v1, v2, 'lop', 3 ]),
//  new Path([], [ v1, v2, 'josh', 4 ])
//]
//
//const pl3 = [
//  new Path([], [ 'daniel', 6 ]),
//  new Path([], [ v1, v2, 'lop', 3 ]),
//  new Path([], [ v1, v2, 'josh', 4 ]),
//  new Path([], [ v1, v2, 'vadas', 5 ]),
//]
//
//const p1 = new Path([],
//  [new Vertex(1, 'person', undefined),
//   new Vertex(2, 'person', undefined)
//  ]);
//
//const p2 = new Path([],
//  [new Vertex(1, 'person', undefined),
//  ]);
//
//const p3 = new Path([],
//   new Vertex(2, 'person', undefined),
//  [new Vertex(1, 'person', undefined),
//  ]);
////console.log(v1)
////console.log(v2)
////console.log(deepMembersById(v1, v2))
////
////console.log(m1)
////console.log(m2)
//
////const m4 = [
////  new Map([["name", "marko"], ["friendRank", 15.0]]),
////  new Map([["name", "vadas"], ["friendRank", 21.0]]),
////  new Map([["name", "lop"], ["friendRank", 15.0]]),
////  new Map([["name", "josh"], ["friendRank", 21.0]]),
////  new Map([["name", "ripple"], ["friendRank", 15.0]]),
////  new Map([["name", "peter"], ["friendRank", 15.0]]),
////];
////
////const m5 = [
////  new Map([["name", "marko"], ["friendRank", 15.0]]),
////  new Map([["name", "vadas"], ["friendRank", 21.0]]),
////  new Map([["name", "lop"], ["friendRank", 15.0]]),
////  new Map([["name", "josh"], ["friendRank", 21.0]]),
////];
//
//
//function assert(expected, actual) {
////    console.log("expected: " + expected, "actual: " + actual);
//    console.log(expected, actual);
//    if (expected !== actual) console.log('!!!!!!!!!!!!!!!!!!!!!!')
//}
//function tests(t, base, missing, swapped) {
//    console.log(t, 'happy ordered');
//        assert(true, deepMembersById(base, base, true));
//        assert(true, deepMembersById(missing, missing, true));
//        assert(true, deepMembersById(swapped, swapped, true));
//    console.log(t, 'missing 1 ordered');
//        assert(false, deepMembersById(base, missing, true));
//    if (t !== "path") {
//    console.log(t, 'swapped ordered');
//        assert(false, deepMembersById(base, swapped, true));
//        }
//    console.log(t, 'happy uo');
//        assert(true, deepMembersById(base, base, false));
//        assert(true, deepMembersById(missing, missing, false));
//        assert(true, deepMembersById(swapped, swapped, false));
//    console.log(t, 'missing 1 uo');
//        assert(false, deepMembersById(base, missing, false));
//    if (t !== "path") {
//    console.log(t, 'swapped uo');
//        assert(true, deepMembersById(base, swapped, false));
//    }
//    console.log(t, 'other')
//        assert(false, deepMembersById(missing, swapped, true));
//        assert(false, deepMembersById(missing, swapped, false));
//}
//
////tests("map", m1, m2, m3)
//tests("array", a1, a2, a3)
////tests("pathlist", pl1, pl2, pl3)
//tests("path", p1, p2, p3)
//
//let t = "array of maps"
////console.log(t, 'missing');
////        assert(false, deepMembersById(m4, m5, false));
////        assert(false, deepMembersById(m4, m5, true));
//
//t = "non objects"
//assert(true, deepMembersById(1, 1, true))
//assert(true, deepMembersById("abc", "abc", true))
//assert(false, deepMembersById(1, "a", true))
//assert(false, deepMembersById(1, 2, true))
//assert(false, deepMembersById(2, "lop", true))
//
//const pr1 = new Property("a", "1");
//const pr2 = new Property("a", "1");
//const aa1 = [1,2,[1,2,3]];
//const aa2 = [1,[2,1,3],2];
//const aaa1 = [1, 2, 3];
//    const aaa2 = [1, 2, 3];
//    const aaa3 = [2, 1, 3];
//console.log(deepMembersById(deepSort(aa1), deepSort(aa2)));
//console.log(deepMembersByIdOrdered(aaa1, aaa3));
//
//const m1 = new Map([
//        ['ripple', [new Vertex(5, 'software', undefined)]],
//        ['peter', [new Vertex(6, 'person', undefined)]],
//        ['vadas', [new Vertex(2, 'person', undefined)]]
//    ]);
//    const m2 = new Map([
//        ['ripple', [new Vertex(5, 'software', undefined)]],
//        ['peter', [new Vertex(6, 'person', undefined)]]
//    ]);
//    const m3 = new Map([
//        ['ripple', [new Vertex(5, 'software', undefined)]],
//        ['vadas', [new Vertex(2, 'person', undefined)]],
//        ['peter', [new Vertex(6, 'person', undefined)]]
//    ]);
//     const m4 = new Map([
//        ['ripple', [new Vertex(5, 'software', undefined)]],
//        ['vadas', [new Vertex(6, 'person', undefined)]],
//        ['peter', [new Vertex(2, 'person', undefined)]]
//    ]);
//    console.log(deepMembersById(m1, m1))
//    console.log(deepMembersById(m1, m2))
// const path1 = new Path(["path"], [new Vertex(1, 'person', undefined),
//                                      new Vertex(2, 'person', undefined)]);
//    const path2 = new Path(["path"], [new Vertex(1, 'person', undefined)]);
//    const path3 = new Path(["path"], [new Vertex(2, 'person', undefined),
//                                      new Vertex(1, 'person', undefined)]);
//    console.log(deepMembersById(path1, path3))
//    console.log([new Vertex(2, 'person', undefined),
//                 new Vertex(1, 'person', undefined)].sort())
