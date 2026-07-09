/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

import { assert } from 'chai';
import { Buffer } from 'buffer';
import ioc from '../../../lib/structure/io/binary/GraphBinary.js';
import StreamReader from '../../../lib/structure/io/binary/internals/StreamReader.js';
import { Tree, Vertex, Edge } from '../../../lib/structure/graph.js';

/**
 * Round-trip tests for the GraphBinary Tree (0x2b) serializer: serialize a Tree
 * with the sync serializer and deserialize it with the async StreamReader path,
 * then assert structure and exercise the read API.
 */
describe('GraphBinary TreeSerializer (0x2b)', () => {
  async function roundTrip(value) {
    const buf = ioc.treeSerializer.serialize(value);
    const reader = StreamReader.fromBuffer(buf);
    return ioc.anySerializer.deserialize(reader);
  }

  // Build a tree:
  //   a -> { b -> { d }, c }
  //   e
  function buildTree() {
    const tree = new Tree();
    const a = tree.getOrCreateChild('a');
    const b = a.getOrCreateChild('b');
    b.getOrCreateChild('d');
    a.getOrCreateChild('c');
    tree.getOrCreateChild('e');
    return tree;
  }

  it('round-trips an empty (leaf) tree', async () => {
    const result = await roundTrip(new Tree());
    assert.instanceOf(result, Tree);
    assert.isTrue(result.isLeaf());
    assert.deepEqual(result.rootNodes(), []);
  });

  it('round-trips a nested tree preserving structure', async () => {
    const original = buildTree();
    const result = await roundTrip(original);

    assert.instanceOf(result, Tree);
    assert.isTrue(result.equals(original));
    assert.deepEqual(result.rootNodes(), ['a', 'e']);
  });

  it('exposes the full read API on the deserialized tree', async () => {
    const result = await roundTrip(buildTree());

    // navigation
    assert.isTrue(result.hasChild('a'));
    assert.isFalse(result.hasChild('z'));
    assert.instanceOf(result.childAt('a'), Tree);
    assert.throws(() => result.childAt('z'));

    // contains is recursive
    assert.isTrue(result.contains('d'));
    assert.isFalse(result.contains('missing'));

    // findSubtree is recursive
    const subD = result.findSubtree('d');
    assert.instanceOf(subD, Tree);
    assert.isTrue(subD.isLeaf());
    assert.isUndefined(result.findSubtree('missing'));

    // structural counts
    assert.equal(result.nodeCount(), 5); // a, b, d, c, e
    assert.deepEqual(result.getNodesAtDepth(0), ['a', 'e']);
    assert.deepEqual(result.getNodesAtDepth(1).sort(), ['b', 'c']);
    assert.deepEqual(result.getNodesAtDepth(2), ['d']);
    assert.deepEqual(result.getNodesAtDepth(99), []);
    assert.deepEqual(result.getNodesAtDepth(-1), []);

    assert.equal(result.getTreesAtDepth(0).length, 1);
    assert.strictEqual(result.getTreesAtDepth(0)[0], result);

    // leaves
    assert.deepEqual(result.getLeafNodes().sort(), ['c', 'd', 'e']);
    assert.equal(result.getLeafTrees().length, 3);

    // splitParents: two roots -> two single-root trees
    const parents = result.splitParents();
    assert.equal(parents.length, 2);
  });

  it('prettyPrint matches the canonical |-- style with no trailing newline', async () => {
    const result = await roundTrip(buildTree());
    const expected = ['|--a', '   |--b', '      |--d', '   |--c', '|--e'].join('\n');
    assert.equal(result.prettyPrint(), expected);
  });

  it('uses value-equality (not reference) for keys', async () => {
    // Two structurally-equal array keys should resolve to the same child.
    const tree = new Tree();
    tree.getOrCreateChild([1, 2]).getOrCreateChild('leaf');
    const child = tree.getOrCreateChild([1, 2]); // same value, different reference
    assert.equal(tree.rootNodes().length, 1);
    assert.isTrue(child.hasChild('leaf'));
  });

  it('Tree.equals is order-insensitive and structural (standalone)', () => {
    // Build two trees with the same structure but sibling keys inserted in a
    // different order; structural equality must ignore order.
    //   a -> { b }, c
    const t1 = new Tree();
    t1.getOrCreateChild('a').getOrCreateChild('b');
    t1.getOrCreateChild('c');

    const t2 = new Tree();
    t2.getOrCreateChild('c');
    t2.getOrCreateChild('a').getOrCreateChild('b');

    assert.isTrue(t1.equals(t2), 'same structure, reversed sibling order should be equal');
    assert.isTrue(t2.equals(t1), 'equals should be symmetric');
    assert.isTrue(t1.equals(t1), 'a tree equals itself');

    // Empty trees are equal.
    assert.isTrue(new Tree().equals(new Tree()));

    // Differing subtree structure -> not equal (key 'x' instead of 'b').
    const tDiff = new Tree();
    tDiff.getOrCreateChild('a').getOrCreateChild('x');
    tDiff.getOrCreateChild('c');
    assert.isFalse(t1.equals(tDiff), 'differing subtree key should not be equal');

    // Differing entry count -> not equal (missing root 'c').
    const tFewer = new Tree();
    tFewer.getOrCreateChild('a').getOrCreateChild('b');
    assert.isFalse(t1.equals(tFewer), 'differing entry count should not be equal');

    // Non-Tree argument -> not equal (and must not throw).
    assert.isFalse(t1.equals(null));
    assert.isFalse(t1.equals(undefined));
    assert.isFalse(t1.equals({}));
    assert.isFalse(t1.equals('a'));
  });

  it('serializes null as a fully-qualified null and deserializes to null', async () => {
    const result = await roundTrip(null);
    assert.isNull(result);
  });

  it('round-trips a null KEY (not a null tree) preserving its subtree', async () => {
    // A null key is a legitimate, distinct key. Nest a child under it and
    // confirm the null key and its subtree survive a GraphBinary round trip.
    const tree = new Tree();
    tree.getOrCreateChild(null).getOrCreateChild('child');

    const result = await roundTrip(tree);
    assert.instanceOf(result, Tree);
    assert.isTrue(result.hasChild(null));
    assert.isTrue(result.childAt(null).hasChild('child'));
  });

  it('collapses duplicate sibling keys into a single entry on deserialize', async () => {    // Hand-craft wire bytes for a tree whose root has the SAME key twice, each
    // carrying a distinct child. A compliant server would never do this, but a
    // hostile/buggy one might; the deserializer must merge them into one entry
    // (matching the Go/Python/.NET GLVs) rather than producing duplicates.
    //   length=2
    //   key='dup', child={ 'a' -> {} }
    //   key='dup', child={ 'b' -> {} }
    const int32 = (n) => ioc.intSerializer.serialize(n, false);
    const childTree = (...keys) => {
      const bufs = [int32(keys.length)];
      for (const k of keys) {
        bufs.push(ioc.anySerializer.serialize(k)); // fully-qualified key
        bufs.push(int32(0)); // bare empty child tree
      }
      return Buffer.concat(bufs);
    };

    const value = Buffer.concat([
      int32(2),
      ioc.anySerializer.serialize('dup'),
      childTree('a'),
      ioc.anySerializer.serialize('dup'),
      childTree('b'),
    ]);
    // Prepend the fully-qualified Tree header: {type_code=0x2b}{value_flag=0x00}.
    const buf = Buffer.concat([Buffer.from([ioc.DataType.TREE, 0x00]), value]);

    const reader = StreamReader.fromBuffer(buf);
    const result = await ioc.anySerializer.deserialize(reader);

    assert.instanceOf(result, Tree);
    // The two 'dup' siblings collapse into a single root entry...
    assert.deepEqual(result.rootNodes(), ['dup']);
    assert.equal(result.rootNodes().length, 1);
    // ...whose subtree is the merge of both children.
    const merged = result.childAt('dup');
    assert.isTrue(merged.hasChild('a'));
    assert.isTrue(merged.hasChild('b'));
  });

  // -------------------------------------------------------------------------
  // Explicit Tree read-API cases, mirroring the common standard exercised by
  // the Java/.NET/Go/Python GLV Tree tests. These were previously bundled into
  // 'exposes the full read API' or absent entirely.
  // -------------------------------------------------------------------------

  it('dedups element keys by id AND concrete type', () => {
    // Two vertices with the same id collapse to one root entry, even when their
    // labels differ - element identity is id-based, not label-based.
    const sameId = new Tree();
    sameId.getOrCreateChild(new Vertex(1, 'person'));
    sameId.getOrCreateChild(new Vertex(1, 'software'));
    assert.equal(sameId.rootNodes().length, 1, 'Vertex(1) twice -> single entry');

    // Two vertices with distinct ids are distinct keys.
    const diffId = new Tree();
    diffId.getOrCreateChild(new Vertex(1, 'person'));
    diffId.getOrCreateChild(new Vertex(2, 'person'));
    assert.equal(diffId.rootNodes().length, 2, 'Vertex(1) and Vertex(2) -> two entries');

    // A Vertex and an Edge sharing the same id are distinct keys: element key
    // equality is concrete-type aware (verifies the treeKeysEqual Part A fix).
    const mixed = new Tree();
    mixed.getOrCreateChild(new Vertex(1, 'person'));
    mixed.getOrCreateChild(new Edge(1, new Vertex(2, 'person'), 'knows', new Vertex(3, 'person')));
    assert.equal(mixed.rootNodes().length, 2, 'Vertex(1) and Edge(1) are different concrete types -> two entries');
  });

  it('childAt throws for a missing key', () => {
    const tree = new Tree();
    tree.getOrCreateChild('present');
    assert.throws(() => tree.childAt('missing'), Error);
  });

  it('addTree(null) throws', () => {
    const tree = new Tree();
    // Merging a null/undefined tree dereferences other.#entries and must throw.
    assert.throws(() => tree.addTree(null));
    assert.throws(() => tree.addTree(undefined));
  });

  it('splitParents returns one tree per root (single, multi, empty)', () => {
    // Single root -> singleton list containing the tree itself.
    const single = new Tree();
    single.getOrCreateChild('a').getOrCreateChild('b');
    const singleParents = single.splitParents();
    assert.equal(singleParents.length, 1);
    assert.strictEqual(singleParents[0], single);

    // Multiple roots -> one single-root tree per root key.
    const multi = new Tree();
    multi.getOrCreateChild('a');
    multi.getOrCreateChild('b');
    multi.getOrCreateChild('c');
    const multiParents = multi.splitParents();
    assert.equal(multiParents.length, 3);
    for (const p of multiParents) {
      assert.equal(p.rootNodes().length, 1);
    }

    // Empty tree -> empty list.
    assert.deepEqual(new Tree().splitParents(), []);
  });

  it('getLeafTrees returns one single-key tree per leaf', () => {
    // Tree: a -> { b -> { d }, c }, e  =>  leaves are d, c, e.
    const tree = new Tree();
    const a = tree.getOrCreateChild('a');
    a.getOrCreateChild('b').getOrCreateChild('d');
    a.getOrCreateChild('c');
    tree.getOrCreateChild('e');

    const leafTrees = tree.getLeafTrees();
    assert.equal(leafTrees.length, 3, 'one tree per leaf key');
    for (const lt of leafTrees) {
      assert.instanceOf(lt, Tree);
      assert.equal(lt.rootNodes().length, 1, 'each leaf tree holds a single key');
    }
    const leafKeys = leafTrees.map((lt) => lt.rootNodes()[0]).sort();
    assert.deepEqual(leafKeys, ['c', 'd', 'e']);
  });
});
