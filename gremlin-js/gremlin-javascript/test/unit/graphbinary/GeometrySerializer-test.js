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
import {
  Model,
  Point,
  LineString,
  Polygon,
  GeometryCollection,
  MultiPoint,
  MultiLineString,
  MultiPolygon,
} from '../../../lib/structure/geo.js';
import ioc, { DataType } from '../../../lib/structure/io/binary/GraphBinary.js';
import StreamReader from '../../../lib/structure/io/binary/internals/StreamReader.js';

const { anySerializer, geometrySerializer } = ioc;

/** POINT(-122.3321 47.6062) under SRID 4326, as the GraphBinary spec writes it. */
const WIRE_EXAMPLE = Buffer.from(
  '31000000' + '10e6' + '00' + '00000001' + 'c05e9541205bc01a' + '4047cd97f62b6ae8',
  'hex',
);

/** Wraps WKB bytes as a fully qualified geometry value with the given SRID. */
function geometryBytes(srid, wkbHex) {
  const sridBytes = Buffer.alloc(4);
  sridBytes.writeInt32BE(srid, 0);
  return Buffer.concat([Buffer.from([DataType.GEOMETRY, 0x00]), sridBytes, Buffer.from(wkbHex, 'hex')]);
}

function read(bytes) {
  return anySerializer.deserialize(StreamReader.fromBuffer(bytes));
}

/** Reads a fully qualified value with the geometry serializer itself. */
function readWithGeometrySerializer(bytes) {
  return geometrySerializer.deserialize(StreamReader.fromBuffer(bytes));
}

async function assertRejects(bytes, errorPattern, readBytes = read) {
  try {
    await readBytes(bytes);
    assert.fail('Expected an error to be thrown');
  } catch (err) {
    assert.match(err.message, errorPattern);
  }
}

describe('GraphBinary.GeometrySerializer', () => {
  describe('#serialize()', () => {
    it('writes the SRID and the ISO WKB of a point', () => {
      const point = Point.of(-122.3321, 47.6062, Model.GEOGRAPHIC);
      assert.deepStrictEqual(geometrySerializer.serialize(point), WIRE_EXAMPLE);
    });

    it('writes a null geometry as a null value flag', () => {
      assert.deepStrictEqual(geometrySerializer.serialize(null), Buffer.from([DataType.GEOMETRY, 0x01]));
      assert.deepStrictEqual(geometrySerializer.serialize(undefined), Buffer.from([DataType.GEOMETRY, 0x01]));
    });

    it('rejects a null geometry without a value flag', () => {
      assert.throws(() => geometrySerializer.serialize(null, false), /null geometry/);
    });

    it('writes NaN coordinates for an empty point', () => {
      const bytes = geometrySerializer.serialize(Point.empty());
      assert.deepStrictEqual(bytes, geometryBytes(0, '00' + '00000001' + '7ff8000000000000'.repeat(2)));
    });
  });

  describe('#deserialize()', () => {
    it('reads the wire example', async () => {
      const point = await read(WIRE_EXAMPLE);
      assert.isTrue(point.equals(Point.of(-122.3321, 47.6062, Model.GEOGRAPHIC)));
      assert.strictEqual(point.model, Model.GEOGRAPHIC);
    });

    it('reads a null value flag as null', async () => {
      assert.isNull(await read(Buffer.from([DataType.GEOMETRY, 0x01])));
    });

    it('reads an empty point', async () => {
      const point = await read(geometryBytes(0, '00' + '00000001' + '7ff8000000000000'.repeat(2)));
      assert.isTrue(point.isEmpty);
      assert.isTrue(point.equals(Point.empty()));
    });

    it('reads three NaN ordinates as an empty point that keeps its z dimension', async () => {
      const bytes = geometryBytes(0, '00' + '000003e9' + '7ff8000000000000'.repeat(3));
      // 2 type code and value flag, 4 SRID, 1 byte order marker, 4 type code and 3 ordinates of 8 bytes
      assert.strictEqual(bytes.length, 35);
      const reader = StreamReader.fromBuffer(bytes);
      const point = await anySerializer.deserialize(reader);
      assert.isTrue(point.isEmpty);
      assert.isTrue(point.hasZ);
      assert.strictEqual(point.toWKT(), 'POINT Z EMPTY');
      assert.deepStrictEqual(anySerializer.serialize(point), bytes);
      // the z ordinate the reader discards is still consumed
      assert.strictEqual(reader.position, bytes.length);
    });

    it('writes big-endian WKB for a little-endian value it read', async () => {
      const x = Buffer.alloc(8);
      x.writeDoubleLE(-122.3321, 0);
      const y = Buffer.alloc(8);
      y.writeDoubleLE(47.6062, 0);
      const wkbHex = '01' + '01000000' + x.toString('hex') + y.toString('hex');
      const point = await read(geometryBytes(4326, wkbHex));
      assert.deepStrictEqual(geometrySerializer.serialize(point), WIRE_EXAMPLE);
    });

    it('stops at the end of the WKB', async () => {
      const trailing = Buffer.from([DataType.INT, 0x00, 0x00, 0x00, 0x00, 0x2a]);
      const reader = StreamReader.fromBuffer(Buffer.concat([WIRE_EXAMPLE, trailing]));
      const point = await anySerializer.deserialize(reader);
      assert.isTrue(point.equals(Point.of(-122.3321, 47.6062, Model.GEOGRAPHIC)));
      assert.strictEqual(await anySerializer.deserialize(reader), 42);
      assert.strictEqual(reader.position, WIRE_EXAMPLE.length + trailing.length);
    });

    it('reads a geometry and an int from a list', async () => {
      const point = Point.of(1, 2);
      const bytes = anySerializer.serialize([point, 42]);
      const reader = StreamReader.fromBuffer(bytes);
      const list = await anySerializer.deserialize(reader);
      assert.strictEqual(list.length, 2);
      assert.isTrue(list[0].equals(point));
      assert.strictEqual(list[1], 42);
      assert.strictEqual(reader.position, bytes.length);
    });

    it('reads its own type code and value flag', async () => {
      const point = await readWithGeometrySerializer(WIRE_EXAMPLE);
      assert.isTrue(point.equals(Point.of(-122.3321, 47.6062, Model.GEOGRAPHIC)));
    });

    it('reads its own null value flag as null', async () => {
      assert.isNull(await readWithGeometrySerializer(Buffer.from([DataType.GEOMETRY, 0x01])));
    });

    it('rejects a value flag that is neither 0x00 nor 0x01', async () => {
      await assertRejects(
        Buffer.from([DataType.GEOMETRY, 0x02]),
        /GeometrySerializer: unexpected \{value_flag\}=0x2/,
        readWithGeometrySerializer,
      );
    });

    it('rejects a type code that is not the geometry type code', async () => {
      await assertRejects(
        Buffer.from([DataType.INT, 0x00]),
        /GeometrySerializer: unexpected \{type_code\}=0x1/,
        readWithGeometrySerializer,
      );
    });
  });

  describe('round trip', () => {
    const values = {
      'a point': Point.of(1, 2),
      'a point with z': Point.ofZ(1, 2, 3),
      'an empty point': Point.empty(),
      'a line string': LineString.of(Point.of(0, 0), Point.of(1, 1), Point.of(2, 0)),
      'a line string with z': LineString.of(Point.ofZ(0, 0, 1), Point.ofZ(1, 1, 2)),
      'an empty line string': LineString.empty(),
      'a polygon with a hole': Polygon.of(
        LineString.of(Point.of(0, 0), Point.of(10, 0), Point.of(10, 10), Point.of(0, 10), Point.of(0, 0)),
        LineString.of(Point.of(2, 2), Point.of(2, 4), Point.of(4, 4), Point.of(2, 2)),
      ),
      'an empty polygon': Polygon.empty(),
      'a multi point': MultiPoint.of(Point.of(1, 2), Point.of(3, 4)),
      'an empty multi point': MultiPoint.empty(),
      'a multi line string': MultiLineString.of(
        LineString.of(Point.of(0, 0), Point.of(1, 1)),
        LineString.of(Point.of(2, 2), Point.of(3, 3)),
      ),
      'an empty multi line string': MultiLineString.empty(),
      'a multi polygon': MultiPolygon.of(
        Polygon.of(LineString.of(Point.of(0, 0), Point.of(1, 0), Point.of(1, 1), Point.of(0, 0))),
      ),
      'an empty multi polygon': MultiPolygon.empty(),
      'a geometry collection': GeometryCollection.of(
        Point.of(1, 2),
        GeometryCollection.of(LineString.of(Point.of(0, 0), Point.of(1, 1))),
        Point.empty(),
      ),
      'an empty geometry collection': GeometryCollection.empty(),
    };

    Object.entries(values).forEach(([name, value]) => {
      it(`keeps ${name}`, async () => {
        const bytes = anySerializer.serialize(value);
        assert.strictEqual(bytes[0], DataType.GEOMETRY);
        const result = await read(bytes);
        assert.strictEqual(result.constructor, value.constructor);
        assert.isTrue(result.equals(value));
        assert.deepStrictEqual(anySerializer.serialize(result), bytes);
      });
    });

    it('keeps the sign of a negative zero coordinate', async () => {
      const bytes = anySerializer.serialize(Point.of(-0, 0));
      assert.include(bytes.toString('hex'), '8000000000000000');
      const point = await read(bytes);
      assert.isTrue(Object.is(point.x, -0));
    });

    it('reports another SRID as OTHER', async () => {
      const point = await read(anySerializer.serialize(Point.of(1, 2, 3857)));
      assert.strictEqual(point.srid, 3857);
      assert.strictEqual(point.model, Model.OTHER);
    });
  });

  describe('malformed input', () => {
    it('rejects a value that ends inside the WKB', async () => {
      await assertRejects(WIRE_EXAMPLE.subarray(0, WIRE_EXAMPLE.length - 3), /Unexpected end of buffer/);
    });

    it('rejects a point count of 0x7fffffff with one point present', async () => {
      // 0x7fffffff is the largest count the reader admits, so the input is
      // rejected for ending early rather than for its count
      const wkbHex = '00' + '00000002' + '7fffffff' + '0000000000000000'.repeat(2);
      await assertRejects(geometryBytes(0, wkbHex), /Unexpected end of buffer/);
    });

    it('rejects a point count of 2^31 or more', async () => {
      const wkbHex = '00' + '00000002' + '80000000' + '0000000000000000'.repeat(2);
      await assertRejects(geometryBytes(0, wkbHex), /Invalid point count: 2147483648/);
    });

    it('rejects a polygon ring with fewer than four points', async () => {
      const noPoints = '00' + '00000003' + '00000001' + '00000000';
      await assertRejects(geometryBytes(0, noPoints), /Polygon ring must have at least 4 points, got 0/);
      // 1 byte order marker, 4 type code, 4 ring count, 4 point count and 3 coordinates of 16 bytes
      const threePoints = '00' + '00000003' + '00000001' + '00000003' + '0000000000000000'.repeat(6);
      assert.strictEqual(threePoints.length / 2, 61);
      await assertRejects(geometryBytes(0, threePoints), /Polygon ring must have at least 4 points, got 3/);
    });

    it('rejects EWKB', async () => {
      const wkbHex = '00' + '80000001' + '3ff0000000000000'.repeat(2);
      await assertRejects(geometryBytes(0, wkbHex), /EWKB format detected \(type code 0x80000001\)/);
      // the M flag and the SRID flag are rejected as the Z flag is
      await assertRejects(
        geometryBytes(0, '00' + '40000001' + '3ff0000000000000'.repeat(2)),
        /EWKB format detected \(type code 0x40000001\)/,
      );
      await assertRejects(
        geometryBytes(0, '00' + '20000001' + '3ff0000000000000'.repeat(2)),
        /EWKB format detected \(type code 0x20000001\)/,
      );
    });

    it('rejects the M and ZM dimensions', async () => {
      await assertRejects(geometryBytes(0, '00' + '000007d1' + '00'.repeat(16)), /M dimension not supported/);
      await assertRejects(geometryBytes(0, '00' + '00000bb9' + '00'.repeat(16)), /ZM dimension not supported/);
    });

    it('rejects an unknown WKB type code', async () => {
      await assertRejects(geometryBytes(0, '00' + '00000008'), /Unknown WKB type code: 8/);
    });

    it('rejects an invalid byte order marker', async () => {
      await assertRejects(geometryBytes(0, '02' + '00000001'), /Invalid WKB byte order: 2/);
    });

    it('rejects a NaN coordinate in a line string', async () => {
      const wkbHex = '00' + '00000002' + '00000002' + '7ff8000000000000' + '0000000000000000' + '00'.repeat(16);
      await assertRejects(geometryBytes(0, wkbHex), /NaN or infinite/);
    });

    it('rejects an empty point as a vertex of a line string or a ring', async () => {
      const empty = '7ff8000000000000'.repeat(2);
      const origin = '0000000000000000'.repeat(2);
      const lineString = '00' + '00000002' + '00000002' + empty + origin;
      await assertRejects(
        geometryBytes(0, lineString),
        /A linestring vertex cannot be an empty point; a vertex must be a real coordinate/,
      );
      const polygon = '00' + '00000003' + '00000001' + '00000004' + empty + origin.repeat(3);
      await assertRejects(
        geometryBytes(0, polygon),
        /A ring vertex cannot be an empty point; a vertex must be a real coordinate/,
      );
    });

    it('rejects a GEOGRAPHIC longitude outside the allowed range', async () => {
      const x = Buffer.alloc(8);
      x.writeDoubleBE(181, 0);
      const wkbHex = '00' + '00000001' + x.toString('hex') + '0000000000000000';
      await assertRejects(geometryBytes(4326, wkbHex), /longitude must be in \[-180, 180\]/);
    });

    it('rejects nesting deeper than 32 levels', async () => {
      const level = '00' + '00000007' + '00000001';
      const point = '00' + '00000001' + '0000000000000000'.repeat(2);
      await assertRejects(geometryBytes(0, level.repeat(33) + point), /nesting depth exceeds maximum of 32/);
      const within = await read(geometryBytes(0, level.repeat(32) + point));
      assert.instanceOf(within, GeometryCollection);
    });

    it('rejects a part that is not of the kind the collection admits', async () => {
      const emptyLineString = '00' + '00000002' + '00000000';
      const origin = '00' + '00000001' + '0000000000000000'.repeat(2);
      const multiPoint = '00' + '00000004' + '00000001' + emptyLineString;
      await assertRejects(geometryBytes(0, multiPoint), /Expected Point in MultiPoint, got LineString/);
      // 1 byte order marker, 4 type code, 4 part count and a point of 21 bytes
      const multiLineString = '00' + '00000005' + '00000001' + origin;
      assert.strictEqual(multiLineString.length / 2, 30);
      await assertRejects(geometryBytes(0, multiLineString), /Expected LineString in MultiLineString, got Point/);
      // 1 byte order marker, 4 type code, 4 part count and an empty line string of 9 bytes
      const multiPolygon = '00' + '00000006' + '00000001' + emptyLineString;
      assert.strictEqual(multiPolygon.length / 2, 18);
      await assertRejects(geometryBytes(0, multiPolygon), /Expected Polygon in MultiPolygon, got LineString/);
    });

    it('rejects WKB that mixes the z dimension', async () => {
      const wkbHex =
        '00' + '00000004' + '00000002' + '00' + '00000001' + '00'.repeat(16) + '00' + '000003e9' + '00'.repeat(24);
      await assertRejects(geometryBytes(0, wkbHex), /Mixed Z dimensions in WKB/);
    });
  });
});
