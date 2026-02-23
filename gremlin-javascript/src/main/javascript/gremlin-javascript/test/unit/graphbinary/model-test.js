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

const { anySerializer } = ioc;

const gbinDir = 'gremlin-test/src/main/resources/org/apache/tinkerpop/gremlin/structure/io/graphbinary/';
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

    it('deserialize .gbin matches model', () => {
      const result = anySerializer.deserialize(fileBytes);
      comparator(result.v, modelValue);
    });

    it('serialize model matches .gbin bytes', () => {
      const serialized = anySerializer.serialize(modelValue);
      assert.deepStrictEqual(serialized, fileBytes);
    });

    it('round-trip serialize(deserialize(fileBytes)) matches .gbin', () => {
      const deserialized = anySerializer.deserialize(fileBytes);
      const reserialized = anySerializer.serialize(deserialized.v);
      assert.deepStrictEqual(reserialized, fileBytes);
    });

    it('length tracking', () => {
      const garbageBytes = Buffer.concat([fileBytes, Buffer.from([0xFF])]);
      const result = anySerializer.deserialize(garbageBytes);
      assert.strictEqual(result.len, fileBytes.length);
    });
  });
}

// object-level: deserialize, double round-trip idempotency, length tracking
function runWriteRead(name, comparator = assertEqual) {
  describe(name, () => {
    const fileBytes = readGbinFile(name);
    const modelValue = model[name];

    it('deserialize .gbin matches model', () => {
      const result = anySerializer.deserialize(fileBytes);
      comparator(result.v, modelValue);
    });

    it('double round-trip idempotency', () => {
      const firstRoundTrip = anySerializer.deserialize(anySerializer.serialize(modelValue));
      const secondRoundTrip = anySerializer.deserialize(anySerializer.serialize(firstRoundTrip.v));
      comparator(firstRoundTrip.v, secondRoundTrip.v);
    });

    it('length tracking', () => {
      const garbageBytes = Buffer.concat([fileBytes, Buffer.from([0xFF])]);
      const result = anySerializer.deserialize(garbageBytes);
      assert.strictEqual(result.len, fileBytes.length);
    });
  });
}

// deserialize-only: deserialize, length tracking
function runRead(name, comparator = assertEqual) {
  describe(name, () => {
    const fileBytes = readGbinFile(name);
    const modelValue = model[name];

    it('deserialize .gbin matches model', () => {
      const result = anySerializer.deserialize(fileBytes);
      comparator(result.v, modelValue);
    });

    it('length tracking', () => {
      const garbageBytes = Buffer.concat([fileBytes, Buffer.from([0xFF])]);
      const result = anySerializer.deserialize(garbageBytes);
      assert.strictEqual(result.len, fileBytes.length);
    });
  });
}

// unimplemented type
function skip(name, reason) {
  describe(name, () => {
    it.skip(`${reason}`, () => {});
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
  // run mode (31 entries)
  run('pos-biginteger');
  run('neg-biginteger');
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
  run('var-type-list');
  run('empty-list');
  run('no-prop-edge');
  run('max-int');
  run('min-int');
  run('min-long');
  run('empty-map');
  run('traversal-path');
  run('empty-path');
  run('edge-property');
  run('null-property');
  run('empty-set');
  run('no-prop-vertex');
  run('id-t');
  run('out-direction');

  // runWriteRead mode (22 entries)
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

  // runRead mode (6 entries)
  runRead('neg-zero-double', negZeroComparator);
  runRead('neg-zero-float', negZeroComparator);
  runRead('max-offsetdatetime', invalidDateComparator);
  runRead('min-offsetdatetime', invalidDateComparator);
  runRead('prop-path');
  runRead('var-type-map');

  // skip mode (8 entries)
  skip('single-byte-char', 'Char type (0x80) not implemented');
  skip('multi-byte-char', 'Char type (0x80) not implemented');
  skip('traversal-tree', 'Tree type not implemented');
  skip('tinker-graph', 'Graph type not implemented');
  skip('pos-bigdecimal', 'BigDecimal type not implemented');
  skip('neg-bigdecimal', 'BigDecimal type not implemented');
  skip('forever-duration', 'Duration type not implemented');
  skip('zero-duration', 'Duration type not implemented');
});