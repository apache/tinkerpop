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
import { END_OF_STREAM } from '../../../lib/structure/io/binary/internals/MarkerSerializer.js';

/**
 * Round-trip tests: serialize a value with the existing sync serializer,
 * then deserialize it with the new async StreamReader-based deserializer.
 */
describe('Async deserialization round-trip', () => {
  async function roundTrip(serializer, value) {
    const buf = serializer.serialize(value);
    const reader = StreamReader.fromBuffer(buf);
    return ioc.anySerializer.deserialize(reader);
  }

  describe('primitives via AnySerializer', () => {
    it('Int', async () => {
      assert.equal(await roundTrip(ioc.intSerializer, 42), 42);
    });

    it('Int negative', async () => {
      assert.equal(await roundTrip(ioc.intSerializer, -2147483648), -2147483648);
    });

    it('Int null', async () => {
      assert.isNull(await roundTrip(ioc.intSerializer, null));
    });

    it('Long', async () => {
      assert.equal(await roundTrip(ioc.longSerializer, 9007199254740991), 9007199254740991);
    });

    it('Short', async () => {
      assert.equal(await roundTrip(ioc.shortSerializer, 32767), 32767);
    });

    it('Byte', async () => {
      assert.equal(await roundTrip(ioc.byteSerializer, -128), -128);
    });

    it('Float', async () => {
      const result = await roundTrip(ioc.floatSerializer, 3.14);
      assert.closeTo(result, 3.14, 0.001);
    });

    it('Double', async () => {
      assert.equal(await roundTrip(ioc.doubleSerializer, 3.141592653589793), 3.141592653589793);
    });

    it('Double NaN', async () => {
      assert.isNaN(await roundTrip(ioc.doubleSerializer, NaN));
    });

    it('Double Infinity', async () => {
      assert.equal(await roundTrip(ioc.doubleSerializer, Infinity), Infinity);
    });

    it('Boolean true', async () => {
      assert.equal(await roundTrip(ioc.booleanSerializer, true), true);
    });

    it('Boolean false', async () => {
      assert.equal(await roundTrip(ioc.booleanSerializer, false), false);
    });

    it('String', async () => {
      assert.equal(await roundTrip(ioc.stringSerializer, 'hello world'), 'hello world');
    });

    it('String empty', async () => {
      assert.equal(await roundTrip(ioc.stringSerializer, ''), '');
    });

    it('String null', async () => {
      assert.isNull(await roundTrip(ioc.stringSerializer, null));
    });

    it('UUID', async () => {
      const uuid = '41d2e28a-20a4-4ab0-b379-d810dede3786';
      assert.equal(await roundTrip(ioc.uuidSerializer, uuid), uuid);
    });

    it('DateTime', async () => {
      const date = new Date('2023-06-15T10:30:00.000Z');
      const result = await roundTrip(ioc.dateTimeSerializer, date);
      assert.equal(result.getTime(), date.getTime());
    });

    it('BigInteger', async () => {
      assert.equal(await roundTrip(ioc.bigIntegerSerializer, 123456789012345678901234567890n), 123456789012345678901234567890n);
    });

    it('BigInteger negative', async () => {
      assert.equal(await roundTrip(ioc.bigIntegerSerializer, -42n), -42n);
    });

    it('Binary', async () => {
      const buf = Buffer.from([0x01, 0x02, 0x03]);
      const result = await roundTrip(ioc.binarySerializer, buf);
      assert.deepEqual([...result], [0x01, 0x02, 0x03]);
    });

    it('null (UnspecifiedNull)', async () => {
      assert.isNull(await roundTrip(ioc.unspecifiedNullSerializer, null));
    });
  });

  describe('Marker', () => {
    it('deserializes EndOfStream marker', async () => {
      // Marker wire format: type_code=0xFD, value_flag=0x00, value=0x00
      const buf = Buffer.from([0xFD, 0x00, 0x00]);
      const reader = StreamReader.fromBuffer(buf);
      const result = await ioc.anySerializer.deserialize(reader);
      assert.equal(result, END_OF_STREAM);
    });
  });

  describe('Enum', () => {
    it('Direction.OUT', async () => {
      const { direction } = await import('../../../lib/process/traversal.js');
      const result = await roundTrip(ioc.enumSerializer, direction.out);
      assert.equal(result.typeName, 'Direction');
      assert.equal(result.elementName, 'OUT');
    });

    it('T.id', async () => {
      const { t } = await import('../../../lib/process/traversal.js');
      const result = await roundTrip(ioc.enumSerializer, t.id);
      assert.equal(result.typeName, 'T');
      assert.equal(result.elementName, 'id');
    });
  });

  describe('direct serializer.deserialize(reader)', () => {
    it('IntSerializer.deserialize reads fully-qualified', async () => {
      const buf = ioc.intSerializer.serialize(99);
      const reader = StreamReader.fromBuffer(buf);
      const result = await ioc.intSerializer.deserialize(reader);
      assert.equal(result, 99);
    });

    it('StringSerializer.deserialize reads fully-qualified', async () => {
      const buf = ioc.stringSerializer.serialize('test');
      const reader = StreamReader.fromBuffer(buf);
      const result = await ioc.stringSerializer.deserialize(reader);
      assert.equal(result, 'test');
    });
  });

  describe('streaming (chunked) deserialization', () => {
    it('deserializes Int from 1-byte chunks', async () => {
      const buf = ioc.intSerializer.serialize(42);
      const chunks = [];
      for (let i = 0; i < buf.length; i++) {
        chunks.push(buf.subarray(i, i + 1));
      }
      const stream = new ReadableStream({
        pull(controller) {
          if (chunks.length > 0) {
            controller.enqueue(new Uint8Array(chunks.shift()));
          } else {
            controller.close();
          }
        },
      });
      const reader = StreamReader.fromReadableStream(stream);
      const result = await ioc.anySerializer.deserialize(reader);
      assert.equal(result, 42);
    });

    it('deserializes String from 2-byte chunks', async () => {
      const buf = ioc.stringSerializer.serialize('hello');
      const chunks = [];
      for (let i = 0; i < buf.length; i += 2) {
        chunks.push(buf.subarray(i, Math.min(i + 2, buf.length)));
      }
      const stream = new ReadableStream({
        pull(controller) {
          if (chunks.length > 0) {
            controller.enqueue(new Uint8Array(chunks.shift()));
          } else {
            controller.close();
          }
        },
      });
      const reader = StreamReader.fromReadableStream(stream);
      const result = await ioc.anySerializer.deserialize(reader);
      assert.equal(result, 'hello');
    });
  });
});

describe('Async deserialization round-trip — compound types', () => {
  async function roundTrip(serializer, value) {
    const buf = serializer.serialize(value);
    const reader = (await import('../../../lib/structure/io/binary/internals/StreamReader.js')).default.fromBuffer(buf);
    return ioc.anySerializer.deserialize(reader);
  }

  it('List of ints', async () => {
    const result = await roundTrip(ioc.listSerializer, [1, 2, 3]);
    assert.deepEqual(result, [1, 2, 3]);
  });

  it('List empty', async () => {
    const result = await roundTrip(ioc.listSerializer, []);
    assert.deepEqual(result, []);
  });

  it('List null', async () => {
    assert.isNull(await roundTrip(ioc.listSerializer, null));
  });

  it('Map', async () => {
    const map = new Map([['a', 1], ['b', 2]]);
    const result = await roundTrip(ioc.mapSerializer, map);
    assert.equal(result.get('a'), 1);
    assert.equal(result.get('b'), 2);
  });

  it('Map empty', async () => {
    const result = await roundTrip(ioc.mapSerializer, new Map());
    assert.equal(result.size, 0);
  });

  it('Nested list of strings', async () => {
    const result = await roundTrip(ioc.listSerializer, ['hello', 'world']);
    assert.deepEqual(result, ['hello', 'world']);
  });

  it('Vertex', async () => {
    const { Vertex } = await import('../../../lib/structure/graph.js');
    const v = new Vertex(1, 'person', []);
    const result = await roundTrip(ioc.vertexSerializer, v);
    assert.equal(result.id, 1);
    assert.equal(result.label, 'person');
  });

  it('Edge', async () => {
    const { Vertex, Edge } = await import('../../../lib/structure/graph.js');
    const outV = new Vertex(1, 'person', []);
    const inV = new Vertex(2, 'person', []);
    const e = new Edge(10, outV, 'knows', inV, []);
    const result = await roundTrip(ioc.edgeSerializer, e);
    assert.equal(result.id, 10);
    assert.equal(result.label, 'knows');
  });

  it('Path', async () => {
    const { Path } = await import('../../../lib/structure/graph.js');
    const p = new Path([['a'], ['b']], [1, 2]);
    const result = await roundTrip(ioc.pathSerializer, p);
    assert.deepEqual(result.labels, [['a'], ['b']]);
    assert.deepEqual(result.objects, [1, 2]);
  });

  it('Property', async () => {
    const { Property } = await import('../../../lib/structure/graph.js');
    const prop = new Property('name', 'marko');
    const result = await roundTrip(ioc.propertySerializer, prop);
    assert.equal(result.key, 'name');
    assert.equal(result.value, 'marko');
  });
});
