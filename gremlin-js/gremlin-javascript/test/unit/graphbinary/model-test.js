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

/*
 * GraphBinaryV4 .gbin reference file validation against expected JS model values.
 *
 * Set the IO_TEST_DIRECTORY environment variable to the directory where
 * the .gbin files that represent the serialized "model" are located.
 */

import { assert } from 'chai';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { model } from './model.js';
import ioc from '../../../lib/structure/io/binary/GraphBinary.js';
import StreamReader from '../../../lib/structure/io/binary/internals/StreamReader.js';

const { anySerializer } = ioc;

const gbinDir = '../../gremlin-test/src/main/resources/org/apache/tinkerpop/gremlin/structure/io/graphbinary/';
const searchPattern = 'gremlin-javascript/src/main';
const thisFile = fileURLToPath(import.meta.url);
const defaultDir = thisFile.substring(0, thisFile.indexOf(searchPattern));
const testResourceDirectory = process.env.IO_TEST_DIRECTORY || (defaultDir + gbinDir);

function readGbinFile(name) {
  return readFileSync(testResourceDirectory + name + '-v4.gbin');
}

// byte-exact: deserialize, serialize, round-trip, length tracking
function run(name, comparator = assertEqual) {
  describe(name, () => {
    const fileBytes = readGbinFile(name);
    const modelValue = model[name];

    it('deserialize .gbin matches model', async () => {
      const result = await anySerializer.deserialize(StreamReader.fromBuffer(fileBytes));
      comparator(result, modelValue);
    });

    it('serialize model matches .gbin bytes', () => {
      const serialized = anySerializer.serialize(modelValue);
      assert.deepStrictEqual(serialized, fileBytes);
    });

    it('round-trip serialize(deserialize(fileBytes)) matches .gbin', async () => {
      const deserialized = await anySerializer.deserialize(StreamReader.fromBuffer(fileBytes));
      const reserialized = anySerializer.serialize(deserialized);
      assert.deepStrictEqual(reserialized, fileBytes);
    });

    it('length tracking', async () => {
      const garbageBytes = Buffer.concat([fileBytes, Buffer.from([0xFF])]);
      const reader = StreamReader.fromBuffer(garbageBytes);
      await anySerializer.deserialize(reader);
      assert.strictEqual(reader.position, fileBytes.length);
    });
  });
}

// object-level: deserialize, double round-trip idempotency, length tracking
function runWriteRead(name, comparator = assertEqual) {
  describe(name, () => {
    const fileBytes = readGbinFile(name);
    const modelValue = model[name];

    it('deserialize .gbin matches model', async () => {
      const result = await anySerializer.deserialize(StreamReader.fromBuffer(fileBytes));
      comparator(result, modelValue);
    });

    it('double round-trip idempotency', async () => {
      const firstRoundTrip = await anySerializer.deserialize(
        StreamReader.fromBuffer(anySerializer.serialize(modelValue)),
      );
      const secondRoundTrip = await anySerializer.deserialize(
        StreamReader.fromBuffer(anySerializer.serialize(firstRoundTrip)),
      );
      comparator(firstRoundTrip, secondRoundTrip);
    });

    it('length tracking', async () => {
      const garbageBytes = Buffer.concat([fileBytes, Buffer.from([0xFF])]);
      const reader = StreamReader.fromBuffer(garbageBytes);
      await anySerializer.deserialize(reader);
      assert.strictEqual(reader.position, fileBytes.length);
    });
  });
}

// deserialize-only: deserialize, length tracking
function runRead(name, comparator = assertEqual) {
  describe(name, () => {
    const fileBytes = readGbinFile(name);
    const modelValue = model[name];

    it('deserialize .gbin matches model', async () => {
      const result = await anySerializer.deserialize(StreamReader.fromBuffer(fileBytes));
      comparator(result, modelValue);
    });

    it('length tracking', async () => {
      const garbageBytes = Buffer.concat([fileBytes, Buffer.from([0xFF])]);
      const reader = StreamReader.fromBuffer(garbageBytes);
      await anySerializer.deserialize(reader);
      assert.strictEqual(reader.position, fileBytes.length);
    });
  });
}

function assertEqual(actual, expected) {
  if (Number.isNaN(expected) && Number.isNaN(actual)) return;
  if (Object.is(expected, -0) && Object.is(actual, -0)) return;
  if (Buffer.isBuffer(expected) && Buffer.isBuffer(actual)) {
    assert.isTrue(expected.equals(actual));
    return;
  }
  if (expected instanceof Set && actual instanceof Set) {
    assert.deepStrictEqual([...expected].sort(), [...actual].sort());
    return;
  }
  if (typeof expected === 'bigint' && typeof actual === 'bigint') {
    assert.strictEqual(actual, expected);
    return;
  }
  assert.deepStrictEqual(actual, expected);
}

function nanComparator(actual, expected) {
  assert.isTrue(Number.isNaN(actual));
  assert.isTrue(Number.isNaN(expected));
}

function negZeroComparator(actual, expected) {
  assert.isTrue(Object.is(actual, -0));
  assert.isTrue(Object.is(expected, -0));
}

function setComparator(actual, expected) {
  assert.instanceOf(actual, Set);
  assert.instanceOf(expected, Set);
  assert.deepStrictEqual([...actual].sort(), [...expected].sort());
}

function orderedMapComparator(actual, expected) {
  assert.instanceOf(actual, Map);
  assert.instanceOf(expected, Map);
  assert.deepStrictEqual([...actual.entries()], [...expected.entries()]);
}

function treeComparator(actual, expected) {
  assert.isTrue(actual.equals(expected));
}

function pathComparator(actual, expected) {
  assert.deepStrictEqual(actual.objects, expected.objects);
  assert.strictEqual(actual.labels.length, expected.labels.length);
  for (let ix = 0; ix < actual.labels.length; ix++) {
    setComparator(actual.labels[ix], expected.labels[ix]);
  }
}

function vertexLabelComparator(actual, expected) {
  assert.strictEqual(actual.id, expected.id);
  setComparator(actual.labels, expected.labels);
}

function propertyComparator(actual, expected) {
  assert.strictEqual(actual.key, expected.key);
  assert.deepStrictEqual(actual.value, expected.value);
}

function vertexPropertyComparator(actual, expected) {
  assert.strictEqual(actual.id, expected.id);
  assert.strictEqual(actual.label, expected.label);
  assert.deepStrictEqual(actual.value, expected.value);
  assert.strictEqual(actual.properties.length, expected.properties.length);
  for (let ix = 0; ix < actual.properties.length; ix++) {
    propertyComparator(actual.properties[ix], expected.properties[ix]);
  }
}

function vertexComparator(actual, expected) {
  assert.strictEqual(actual.id, expected.id);
  assert.strictEqual(actual.label, expected.label);
  setComparator(actual.labels, expected.labels);
  assert.strictEqual(actual.properties.length, expected.properties.length);
  for (let ix = 0; ix < actual.properties.length; ix++) {
    vertexPropertyComparator(actual.properties[ix], expected.properties[ix]);
  }
}

function edgeComparator(actual, expected) {
  assert.strictEqual(actual.id, expected.id);
  assert.strictEqual(actual.label, expected.label);
  setComparator(actual.labels, expected.labels);
  assert.strictEqual(actual.outV.id, expected.outV.id);
  assert.strictEqual(actual.inV.id, expected.inV.id);
  assert.strictEqual(actual.properties.length, expected.properties.length);
  for (let ix = 0; ix < actual.properties.length; ix++) {
    propertyComparator(actual.properties[ix], expected.properties[ix]);
  }
}

function graphComparator(actual, expected) {
  assert.strictEqual(actual.vertices.size, expected.vertices.size);
  assert.strictEqual(actual.edges.size, expected.edges.size);
  assert.deepStrictEqual([...actual.vertices.keys()], [...expected.vertices.keys()]);
  assert.deepStrictEqual([...actual.edges.keys()], [...expected.edges.keys()]);
  for (const [id, expectedVertex] of expected.vertices) {
    vertexComparator(actual.vertices.get(id), expectedVertex);
  }
  for (const [id, expectedEdge] of expected.edges) {
    edgeComparator(actual.edges.get(id), expectedEdge);
  }
}

function primitivePdtComparator(actual, expected) {
  assert.strictEqual(actual.name, expected.name);
  assert.strictEqual(actual.value, expected.value);
}

function compositePdtComparator(actual, expected) {
  assert.strictEqual(actual.name, expected.name);
  assert.deepStrictEqual(actual.fields, expected.fields);
}

function setCardinalityComparator(actual, expected) {
  assert.strictEqual(actual.id, expected.id);
  assert.strictEqual(actual.label, expected.label);
  assert.instanceOf(actual.value, Set);
  assert.instanceOf(expected.value, Set);
  assert.deepStrictEqual([...actual.value].sort(), [...expected.value].sort());
  assert.deepStrictEqual(actual.properties, expected.properties);
}

function invalidDateComparator(actual, expected) {
  assert.isTrue(actual instanceof Date);
  assert.isTrue(expected instanceof Date);
  assert.isTrue(Number.isNaN(actual.getTime()));
  assert.isTrue(Number.isNaN(expected.getTime()));
}

describe('GraphBinary v4 Model Tests', () => {
  // run mode
  run('pos-biginteger');
  run('neg-biginteger');
  run('zero-biginteger');
  run('sign-boundary-pos-biginteger');
  run('sign-boundary-neg-biginteger');
  run('uint8-primitive-pdt', primitivePdtComparator);
  run('point-composite-pdt', compositePdtComparator);
  run('empty-binary');
  run('str-binary');
  run('max-double');
  run('min-double');
  run('neg-max-double');
  run('neg-min-double');
  run('nan-double', nanComparator);
  run('pos-inf-double');
  run('neg-inf-double');
  run('unspecified-null');
  run('true-boolean');
  run('false-boolean');
  run('single-byte-string');
  run('mixed-string');
  run('empty-string');
  run('var-type-list');
  run('empty-list');
  run('no-prop-edge');
  run('max-int');
  run('min-int');
  run('empty-map');
  run('traversal-path');
  run('empty-path');
  run('path-zero-labels', pathComparator);
  run('empty-tree', treeComparator);
  run('tree-null-key', treeComparator);
  run('tree-mixed-key-types', treeComparator);
  run('tree-deep-nesting', treeComparator);
  run('edge-property');
  run('null-property');
  run('empty-set');
  run('no-prop-vertex');
  run('multi-label-vertex', vertexLabelComparator);
  run('empty-label-vertex', vertexLabelComparator);
  run('id-t');
  run('out-direction');
  run('merge-on-create');
  run('merge-on-match');
  run('merge-out-v');
  run('merge-in-v');
  run('neg-zero-double', negZeroComparator);

  // runWriteRead mode
  runWriteRead('min-byte');
  runWriteRead('max-byte');
  runWriteRead('max-float');
  runWriteRead('min-float');
  runWriteRead('neg-max-float');
  runWriteRead('neg-min-float');
  runWriteRead('nan-float', nanComparator);
  runWriteRead('pos-inf-float');
  runWriteRead('neg-inf-float');
  runWriteRead('var-bulklist');
  runWriteRead('empty-bulklist');
  runWriteRead('traversal-edge');
  runWriteRead('min-long');
  runWriteRead('max-long');
  runWriteRead('var-type-set', setComparator);
  runWriteRead('max-short');
  runWriteRead('min-short');
  runWriteRead('specified-uuid');
  runWriteRead('nil-uuid');
  runWriteRead('traversal-vertexproperty');
  runWriteRead('meta-vertexproperty');
  runWriteRead('set-cardinality-vertexproperty', setCardinalityComparator);
  runWriteRead('traversal-vertex');
  // JS deserializes safe Long values to Number, so vertex-property ids can't be re-emitted byte-exactly.
  runWriteRead('tinker-graph', graphComparator);
  // label set ordering isn't guaranteed after a JS object round trip.
  runWriteRead('path-multiple-labels', pathComparator);
  // JS preserves Map entry order, but its writer doesn't emit the ordered-map value flag.
  runWriteRead('ordered-string-int-map', orderedMapComparator);
  runWriteRead('traversal-tree', treeComparator); // vertex properties aren't serialized

  // runRead mode
  // JS Number can't re-emit this as a Float.
  runRead('neg-zero-float', negZeroComparator);
  // invalid Date models can't be serialized back.
  runRead('max-offsetdatetime', invalidDateComparator);
  runRead('min-offsetdatetime', invalidDateComparator);
  // properties aren't serialized in JS for this path fixture.
  runRead('prop-path');
  // this fixture has complex keys that JS can't write back byte-exactly.
  runRead('var-type-map');
  // typed nulls deserialize to plain null, so the original type can't be re-emitted.
  runRead('null-int');
  runRead('null-long');
  runRead('null-string');
  runRead('null-list');
  runRead('null-map');
  runRead('null-set');
});
