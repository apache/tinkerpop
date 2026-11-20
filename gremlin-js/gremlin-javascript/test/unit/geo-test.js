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
  Geometry,
  Curve,
  Surface,
  MultiCurve,
  MultiSurface,
  Point,
  LineString,
  Polygon,
  GeometryCollection,
  MultiPoint,
  MultiLineString,
  MultiPolygon,
} from '../../lib/structure/geo.js';

/** The polygon with a hole used by the shared IO fixtures. */
function polygonWithHole() {
  const shell = LineString.of(
    Point.of(0, 0, Model.GEOGRAPHIC),
    Point.of(10, 0, Model.GEOGRAPHIC),
    Point.of(10, 10, Model.GEOGRAPHIC),
    Point.of(0, 10, Model.GEOGRAPHIC),
    Point.of(0, 0, Model.GEOGRAPHIC),
  );
  const hole = LineString.of(
    Point.of(2, 2, Model.GEOGRAPHIC),
    Point.of(2, 4, Model.GEOGRAPHIC),
    Point.of(4, 4, Model.GEOGRAPHIC),
    Point.of(2, 2, Model.GEOGRAPHIC),
  );
  return Polygon.of(shell, hole);
}

/** The nested collection used by the shared IO fixtures. */
function nestedCollection() {
  return GeometryCollection.of(
    Point.of(1, 2),
    GeometryCollection.of(LineString.of(Point.of(0, 0), Point.of(1, 1))),
    Point.empty(),
  );
}

describe('Geometry', () => {
  describe('Point', () => {
    it('keeps the coordinates and the model it was given', () => {
      const point = Point.of(-122.3321, 47.6062, Model.GEOGRAPHIC);
      assert.strictEqual(point.x, -122.3321);
      assert.strictEqual(point.y, 47.6062);
      assert.strictEqual(point.srid, 4326);
      assert.strictEqual(point.model, Model.GEOGRAPHIC);
      assert.isFalse(point.hasZ);
      assert.isFalse(point.isEmpty);
    });

    it('keeps a z coordinate', () => {
      const point = Point.ofZ(1, 2, 3);
      assert.strictEqual(point.z, 3);
      assert.isTrue(point.hasZ);
      assert.strictEqual(point.model, Model.CARTESIAN);
      assert.strictEqual(Point.ofZ(1, 2, 3, Model.GEOGRAPHIC).srid, 4326);
    });

    it('reports a missing z as undefined', () => {
      const point = Point.of(1, 2);
      assert.isUndefined(point.z);
      assert.isFalse(point.hasZ);
    });

    it('holds no coordinate when empty', () => {
      const point = Point.empty();
      assert.isTrue(point.isEmpty);
      assert.throws(() => point.x, /empty point/);
      assert.throws(() => point.y, /empty point/);
      assert.throws(() => point.z, /empty point/);
    });

    it('rejects a coordinate that is NaN or infinite', () => {
      assert.throws(() => Point.of(NaN, 2), /NaN or infinite/);
      assert.throws(() => Point.of(1, Infinity), /NaN or infinite/);
      assert.throws(() => Point.ofZ(1, 2, NaN), /z coordinate cannot be NaN/);
      assert.throws(() => Point.ofZ(1, 2, -Infinity), /z coordinate cannot be infinite/);
    });

    it('rejects a coordinate that is not a number', () => {
      assert.throws(() => Point.of('1', 2), /x coordinate must be a number/);
      assert.throws(() => Point.of(1, true), /y coordinate must be a number/);
      assert.throws(() => Point.ofZ(1, 2, '3'), /z coordinate must be a number/);
    });

    it('bounds GEOGRAPHIC coordinates inclusively', () => {
      assert.throws(() => Point.of(180.5, 0, Model.GEOGRAPHIC), /longitude must be in \[-180, 180\]/);
      assert.throws(() => Point.of(0, -90.5, Model.GEOGRAPHIC), /latitude must be in \[-90, 90\]/);
      assert.strictEqual(Point.of(180, -90, Model.GEOGRAPHIC).x, 180);
      assert.strictEqual(Point.of(-180, 90, Model.GEOGRAPHIC).y, 90);
    });

    it('accepts any other SRID and reports it as OTHER', () => {
      const point = Point.of(1, 2, 3857);
      assert.strictEqual(point.srid, 3857);
      assert.strictEqual(point.model, Model.OTHER);
    });

    it('rejects Model.OTHER', () => {
      assert.throws(() => Point.of(1, 2, Model.OTHER), /Model.OTHER cannot be used/);
      assert.throws(() => Point.empty(Model.OTHER), /Model.OTHER cannot be used/);
      assert.throws(() => LineString.empty(Model.OTHER), /Model.OTHER cannot be used/);
    });

    it('rejects an SRID that is not an integer in the signed 32-bit range', () => {
      assert.throws(() => Point.of(1, 2, 2 ** 31), /signed 32-bit range/);
      assert.throws(() => Point.of(1, 2, -(2 ** 31) - 1), /signed 32-bit range/);
      assert.throws(() => Point.of(1, 2, 3.5), /signed 32-bit range/);
      assert.strictEqual(Point.of(1, 2, 2 ** 31 - 1).srid, 2147483647);
      assert.strictEqual(Point.of(1, 2, -(2 ** 31)).srid, -2147483648);
    });
  });

  describe('LineString', () => {
    it('needs either no points or at least two', () => {
      assert.throws(() => LineString.of(Point.of(1, 2)), /at least 2 points, got 1/);
      assert.throws(() => LineString.of(), /at least 2 points, got 0/);
      assert.isTrue(LineString.empty().isEmpty);
      assert.deepStrictEqual([...LineString.empty().points], []);
    });

    it('rejects points that mix 2D and 3D', () => {
      assert.throws(() => LineString.of(Point.of(0, 0), Point.ofZ(1, 1, 1)), /Mixed z dimensions in LineString/);
    });

    it('rejects an empty point as a vertex', () => {
      assert.throws(() => LineString.of(Point.of(0, 0), Point.empty()), /must be a real coordinate/);
    });

    it('rejects a vertex that is not a point', () => {
      const lineString = LineString.of(Point.of(0, 0), Point.of(1, 1));
      assert.throws(
        () => LineString.of(lineString, lineString),
        /LineString points element at index 0 must be a Point, got: LineString/,
      );
    });

    it('rejects points of different SRIDs', () => {
      assert.throws(() => LineString.of(Point.of(0, 0), Point.of(1, 1, Model.GEOGRAPHIC)), /Mixed SRIDs in LineString/);
    });
  });

  describe('Polygon', () => {
    it('needs rings that are closed and hold at least four points', () => {
      const open = LineString.of(Point.of(0, 0), Point.of(1, 0), Point.of(1, 1), Point.of(0, 1));
      assert.throws(() => Polygon.of(open), /must be closed/);
      const tooShort = LineString.of(Point.of(0, 0), Point.of(1, 1), Point.of(0, 0));
      assert.throws(() => Polygon.of(tooShort), /at least 4 points/);
    });

    it('rejects an empty hole and holes on an empty shell', () => {
      const shell = LineString.of(Point.of(0, 0), Point.of(1, 0), Point.of(1, 1), Point.of(0, 0));
      assert.throws(() => Polygon.of(shell, LineString.empty()), /Hole ring at index 0 cannot be empty/);
      assert.throws(() => Polygon.of(LineString.empty(), shell), /empty shell cannot have holes/);
    });

    it('rejects a null hole', () => {
      const shell = LineString.of(Point.of(0, 0), Point.of(1, 0), Point.of(1, 1), Point.of(0, 0));
      assert.throws(() => Polygon.of(shell, null), /Polygon holes element at index 0 cannot be null/);
      assert.throws(() => Polygon.of(shell, undefined), /Polygon holes element at index 0 cannot be null/);
    });

    it('rejects a shell that is not a line string', () => {
      assert.throws(() => Polygon.of(Point.empty()), /Shell ring must be a LineString, got: Point/);
    });

    it('exposes its rings', () => {
      const polygon = polygonWithHole();
      assert.strictEqual(polygon.exteriorRing.points.length, 5);
      assert.strictEqual(polygon.interiorRings.length, 1);
      assert.strictEqual(polygon.model, Model.GEOGRAPHIC);
      assert.isFalse(polygon.isEmpty);
      assert.isTrue(Polygon.empty().isEmpty);
    });
  });

  describe('collections', () => {
    it('exposes the parts of a multi-kind', () => {
      const multiPoint = MultiPoint.of(Point.of(1, 2), Point.of(3, 4));
      assert.strictEqual(multiPoint.geometries.length, 2);
      assert.isTrue(multiPoint.geometries[0].equals(Point.of(1, 2)));
    });

    it('rejects parts of different SRIDs', () => {
      const here = Point.of(1, 2);
      const there = Point.of(3, 4, Model.GEOGRAPHIC);
      assert.throws(() => MultiPoint.of(here, there), /Mixed SRIDs in MultiPoint/);
      assert.throws(() => GeometryCollection.of(here, there), /Mixed SRIDs in GeometryCollection/);
      assert.throws(
        () =>
          MultiLineString.of(
            LineString.of(here, Point.of(2, 3)),
            LineString.of(there, Point.of(4, 5, Model.GEOGRAPHIC)),
          ),
        /Mixed SRIDs in MultiLineString/,
      );
    });

    it('needs at least one part', () => {
      assert.throws(() => MultiPoint.of(), /at least 1 part/);
      assert.throws(() => GeometryCollection.of(), /at least 1 part/);
      assert.isTrue(MultiPolygon.empty().isEmpty);
    });

    it('rejects a part of another kind in a multi-point', () => {
      const lineString = LineString.of(Point.of(0, 0), Point.of(1, 1));
      assert.throws(
        () => MultiPoint.of(lineString),
        /MultiPoint points element at index 0 must be a Point, got: LineString/,
      );
    });

    it('rejects a part of another kind in a multi-line string', () => {
      assert.throws(
        () => MultiLineString.of(Point.of(1, 2)),
        /MultiLineString line strings element at index 0 must be a LineString, got: Point/,
      );
    });

    it('rejects a part of another kind in a multi-polygon', () => {
      assert.throws(
        () => MultiPolygon.of(Point.of(1, 2)),
        /MultiPolygon polygons element at index 0 must be a Polygon, got: Point/,
      );
    });

    it('rejects a part that is not a geometry in a collection', () => {
      assert.throws(
        () => GeometryCollection.of({ x: 1, y: 2 }),
        /GeometryCollection geometries element at index 0 must be a Geometry, got: Object/,
      );
    });

    it('is empty when every part is empty', () => {
      assert.isTrue(MultiPoint.of(Point.empty(), Point.empty()).isEmpty);
      assert.isFalse(MultiPoint.of(Point.empty(), Point.of(1, 2)).isEmpty);
    });

    it('allows nesting of up to 32 levels', () => {
      let nested = GeometryCollection.of(Point.of(0, 0));
      for (let depth = 2; depth <= 32; depth++) {
        nested = GeometryCollection.of(nested);
      }
      assert.strictEqual(nested.geometries.length, 1);
      assert.throws(() => GeometryCollection.of(nested), /nesting depth exceeds maximum of 32/);
    });
  });

  describe('#equals()', () => {
    it('is true for the same values', () => {
      assert.isTrue(Point.of(1, 2).equals(Point.of(1, 2)));
      assert.isTrue(Point.ofZ(1, 2, 3).equals(Point.ofZ(1, 2, 3)));
      assert.isTrue(polygonWithHole().equals(polygonWithHole()));
      assert.isTrue(nestedCollection().equals(nestedCollection()));
    });

    it('treats -0 as 0', () => {
      assert.isTrue(Point.of(-0, 0).equals(Point.of(0, 0)));
      assert.isTrue(Point.ofZ(1, 2, -0).equals(Point.ofZ(1, 2, 0)));
    });

    it('is false for a different z coordinate', () => {
      assert.isFalse(Point.ofZ(1, 2, 3).equals(Point.ofZ(1, 2, 4)));
      assert.isFalse(Point.ofZ(1, 2, 4).equals(Point.ofZ(1, 2, 3)));
    });

    it('is false for collections that hold a different number of parts', () => {
      const here = Point.of(1, 2);
      const there = Point.of(3, 4);
      assert.isFalse(MultiPoint.of(here).equals(MultiPoint.of(here, there)));
      assert.isFalse(MultiPoint.of(here, there).equals(MultiPoint.of(here)));
    });

    it('is true for two empty points of the same SRID', () => {
      assert.isTrue(Point.empty().equals(Point.empty()));
      assert.isTrue(Point.empty(Model.GEOGRAPHIC).equals(Point.empty(Model.GEOGRAPHIC)));
      assert.isFalse(Point.empty().equals(Point.empty(Model.GEOGRAPHIC)));
      assert.isFalse(Point.empty().equals(Point.of(0, 0)));
    });

    it('is false for a different SRID, z dimension or kind', () => {
      assert.isFalse(Point.of(1, 2).equals(Point.of(1, 2, Model.GEOGRAPHIC)));
      assert.isFalse(Point.of(1, 2).equals(Point.ofZ(1, 2, 0)));
      const first = Point.of(0, 0);
      const second = Point.of(1, 1);
      assert.isFalse(LineString.of(first, second).equals(MultiPoint.of(first, second)));
      assert.isFalse(GeometryCollection.of(first, second).equals(MultiPoint.of(first, second)));
    });

    it('is false for a value that is not a geometry', () => {
      assert.isFalse(Point.of(1, 2).equals(null));
      assert.isFalse(Point.of(1, 2).equals({ x: 1, y: 2 }));
    });
  });

  describe('#toWKT()', () => {
    it('writes a point', () => {
      assert.strictEqual(Point.of(-122.3321, 47.6062, Model.GEOGRAPHIC).toWKT(), 'POINT(-122.3321 47.6062)');
      assert.strictEqual(Point.ofZ(1, 2, 3).toWKT(), 'POINT Z (1 2 3)');
      assert.strictEqual(Point.empty().toWKT(), 'POINT EMPTY');
    });

    it('writes a polygon with a hole', () => {
      assert.strictEqual(polygonWithHole().toWKT(), 'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 4, 4 4, 2 2))');
    });

    it('writes a multi polygon', () => {
      const first = Polygon.of(
        LineString.of(Point.of(0, 0), Point.of(1, 0), Point.of(1, 1), Point.of(0, 1), Point.of(0, 0)),
      );
      const second = Polygon.of(
        LineString.of(Point.of(2, 2), Point.of(3, 2), Point.of(3, 3), Point.of(2, 3), Point.of(2, 2)),
      );
      assert.strictEqual(
        MultiPolygon.of(first, second).toWKT(),
        'MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))',
      );
    });

    it('writes a nested collection', () => {
      assert.strictEqual(
        nestedCollection().toWKT(),
        'GEOMETRYCOLLECTION(POINT(1 2), GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1)), POINT EMPTY)',
      );
    });

    it('writes an empty part of a multi-kind as the EMPTY keyword', () => {
      assert.strictEqual(MultiPoint.of(Point.of(1, 2), Point.empty()).toWKT(), 'MULTIPOINT((1 2), EMPTY)');
      assert.strictEqual(MultiLineString.of(LineString.empty()).toWKT(), 'MULTILINESTRING(EMPTY)');
    });

    it('writes the shortest decimal in plain notation', () => {
      assert.strictEqual(Point.of(0.1, 1e15).toWKT(), 'POINT(0.1 1000000000000000)');
      assert.strictEqual(Point.of(1e16, 1e21).toWKT(), 'POINT(10000000000000000 1000000000000000000000)');
      assert.strictEqual(Point.of(1e-5, 1e-6).toWKT(), 'POINT(0.00001 0.000001)');
      assert.strictEqual(Point.of(1e-7, -0).toWKT(), 'POINT(0.0000001 -0)');
    });

    it('is what the string form gives', () => {
      const polygon = polygonWithHole();
      assert.strictEqual(polygon.toString(), polygon.toWKT());
      assert.strictEqual(`${Point.of(1, 2)}`, 'POINT(1 2)');
    });
  });

  describe('#toEWKT()', () => {
    it('prefixes the WKT with the SRID', () => {
      assert.strictEqual(Point.of(1, 2, 3857).toEWKT(), 'SRID=3857;POINT(1 2)');
      assert.strictEqual(Point.of(1, 2).toEWKT(), 'SRID=0;POINT(1 2)');
    });
  });

  describe('#toWKB()', () => {
    it('writes big-endian ISO WKB', () => {
      const wkb = Point.of(-122.3321, 47.6062, Model.GEOGRAPHIC).toWKB();
      assert.strictEqual(wkb.toString('hex'), '0000000001c05e9541205bc01a4047cd97f62b6ae8');
    });

    it('writes the ISO Z type code and three ordinates for a 3D point', () => {
      assert.strictEqual(
        Point.ofZ(1, 2, 3).toWKB().toString('hex'),
        '00000003e93ff000000000000040000000000000004008000000000000',
      );
    });

    it('writes NaN coordinates for an empty point', () => {
      assert.strictEqual(Point.empty().toWKB().toString('hex'), '00000000017ff80000000000007ff8000000000000');
    });

    it('keeps the sign of a negative zero', () => {
      assert.strictEqual(Point.of(-0, 0).toWKB().toString('hex'), '000000000180000000000000000000000000000000');
    });
  });

  describe('.fromWKB()', () => {
    const values = {
      'a point': Point.of(1, 2),
      'a point with z': Point.ofZ(1, 2, 3),
      'an empty point': Point.empty(),
      'a line string': LineString.of(Point.of(0, 0), Point.of(1, 1), Point.of(2, 0)),
      'an empty line string': LineString.empty(),
      'a polygon with a hole': polygonWithHole(),
      'an empty polygon': Polygon.empty(),
      'a multi point': MultiPoint.of(Point.of(1, 2), Point.empty()),
      'a multi line string': MultiLineString.of(
        LineString.of(Point.of(0, 0), Point.of(1, 1)),
        LineString.of(Point.of(2, 2), Point.of(3, 3)),
      ),
      'a multi polygon': MultiPolygon.of(
        Polygon.of(LineString.of(Point.of(0, 0), Point.of(1, 0), Point.of(1, 1), Point.of(0, 0))),
      ),
      'a nested collection': nestedCollection(),
    };

    Object.entries(values).forEach(([name, value]) => {
      it(`reads back ${name}`, () => {
        const result = Geometry.fromWKB(value.toWKB(), value.srid);
        assert.strictEqual(result.constructor, value.constructor);
        assert.isTrue(result.equals(value));
      });
    });

    it('applies the SRID it is given', () => {
      const point = Geometry.fromWKB(Point.of(1, 2).toWKB(), 4326);
      assert.strictEqual(point.srid, 4326);
      assert.strictEqual(point.model, Model.GEOGRAPHIC);
    });

    it('reads little-endian input', () => {
      const wkb = Buffer.alloc(21);
      wkb.writeUInt8(0x01, 0);
      wkb.writeInt32LE(1, 1);
      wkb.writeDoubleLE(1, 5);
      wkb.writeDoubleLE(2, 13);
      assert.isTrue(Geometry.fromWKB(wkb, 0).equals(Point.of(1, 2)));
    });

    /** The byte order marker and the ISO type code of a geometry. */
    function wkbHeader(littleEndian, typeCode) {
      const header = Buffer.alloc(5);
      header.writeUInt8(littleEndian ? 0x01 : 0x00, 0);
      if (littleEndian) {
        header.writeInt32LE(typeCode, 1);
      } else {
        header.writeInt32BE(typeCode, 1);
      }
      return header;
    }

    /** A count of parts, in the byte order of the geometry that declares it. */
    function wkbCount(littleEndian, count) {
      const bytes = Buffer.alloc(4);
      if (littleEndian) {
        bytes.writeUInt32LE(count, 0);
      } else {
        bytes.writeUInt32BE(count, 0);
      }
      return bytes;
    }

    /** A whole point geometry, header and coordinates, in one byte order. */
    function wkbPoint(littleEndian, x, y) {
      const coordinates = Buffer.alloc(16);
      if (littleEndian) {
        coordinates.writeDoubleLE(x, 0);
        coordinates.writeDoubleLE(y, 8);
      } else {
        coordinates.writeDoubleBE(x, 0);
        coordinates.writeDoubleBE(y, 8);
      }
      return Buffer.concat([wkbHeader(littleEndian, 1), coordinates]);
    }

    it('reads the byte order each nested geometry declares', () => {
      const expected = MultiPoint.of(Point.of(1, 2), Point.of(3, 4));
      [
        [true, false],
        [false, true],
      ].forEach(([headerLittleEndian, partLittleEndian]) => {
        const wkb = Buffer.concat([
          wkbHeader(headerLittleEndian, 4),
          wkbCount(headerLittleEndian, 2),
          wkbPoint(partLittleEndian, 1, 2),
          wkbPoint(partLittleEndian, 3, 4),
        ]);
        assert.isTrue(Geometry.fromWKB(wkb, 0).equals(expected));
      });
    });

    it('rejects bytes that follow the geometry', () => {
      const wkb = Buffer.concat([Point.of(1, 2).toWKB(), Buffer.from([0x00])]);
      assert.throws(() => Geometry.fromWKB(wkb, 0), /Unexpected trailing bytes/);
    });

    it('rejects input that ends inside the geometry', () => {
      const wkb = Point.of(1, 2).toWKB();
      assert.throws(() => Geometry.fromWKB(wkb.subarray(0, wkb.length - 1), 0), /Truncated WKB input/);
      assert.throws(() => Geometry.fromWKB(Buffer.from('00', 'hex'), 0), /Truncated WKB input/);
    });

    it('rejects EWKB', () => {
      const wkb = Buffer.from('00' + '80000001' + '3ff0000000000000'.repeat(2), 'hex');
      assert.throws(() => Geometry.fromWKB(wkb, 0), /EWKB/);
    });

    it('rejects a point with one NaN ordinate', () => {
      // only NaN in both x and y encodes an empty point
      const nanX = wkbPoint(false, NaN, 2);
      assert.strictEqual(nanX.length, 21);
      assert.throws(() => Geometry.fromWKB(nanX, 0), /x coordinate cannot be NaN or infinite/);
      const nanY = wkbPoint(false, 1, NaN);
      assert.strictEqual(nanY.length, 21);
      assert.throws(() => Geometry.fromWKB(nanY, 0), /y coordinate cannot be NaN or infinite/);
    });

    it('rejects input that is not a non-empty byte array', () => {
      assert.throws(() => Geometry.fromWKB(Buffer.alloc(0), 0), /cannot be empty/);
      assert.throws(() => Geometry.fromWKB(null, 0), /must be a Uint8Array/);
    });
  });

  describe('PostGIS WKB', () => {
    /** POLYGON((10 20, 30 20, 30 40, 10 40, 10 20)) with SRID 4326. */
    function polygon() {
      return Polygon.of(
        LineString.of(
          Point.of(10, 20, Model.GEOGRAPHIC),
          Point.of(30, 20, Model.GEOGRAPHIC),
          Point.of(30, 40, Model.GEOGRAPHIC),
          Point.of(10, 40, Model.GEOGRAPHIC),
          Point.of(10, 20, Model.GEOGRAPHIC),
        ),
      );
    }

    // The form PostGIS writes for ST_AsBinary with its default byte order.
    const asBinaryHex =
      '01' +
      '03000000' +
      '01000000' +
      '05000000' +
      '0000000000002440' +
      '0000000000003440' +
      '0000000000003e40' +
      '0000000000003440' +
      '0000000000003e40' +
      '0000000000004440' +
      '0000000000002440' +
      '0000000000004440' +
      '0000000000002440' +
      '0000000000003440';

    // The form PostGIS writes for ST_AsBinary when asked for XDR byte order.
    const xdrHex =
      '00' +
      '00000003' +
      '00000001' +
      '00000005' +
      '4024000000000000' +
      '4034000000000000' +
      '403e000000000000' +
      '4034000000000000' +
      '403e000000000000' +
      '4044000000000000' +
      '4024000000000000' +
      '4044000000000000' +
      '4024000000000000' +
      '4034000000000000';

    // The form PostGIS writes for ST_AsEWKB, which carries an SRID flag.
    const ewkbHex =
      '01' +
      '03000020' +
      'e6100000' +
      '01000000' +
      '05000000' +
      '0000000000002440' +
      '0000000000003440' +
      '0000000000003e40' +
      '0000000000003440' +
      '0000000000003e40' +
      '0000000000004440' +
      '0000000000002440' +
      '0000000000004440' +
      '0000000000002440' +
      '0000000000003440';

    it('reads the bytes PostGIS writes for ST_AsBinary', () => {
      const asBinary = Buffer.from(asBinaryHex, 'hex');
      assert.strictEqual(asBinary.length, 93);

      const read = Geometry.fromWKB(asBinary, 4326);
      assert.isTrue(read.equals(polygon()));
      assert.strictEqual(read.srid, 4326);
      assert.strictEqual(read.model, Model.GEOGRAPHIC);
    });

    it('writes the bytes PostGIS writes for ST_AsBinary with XDR', () => {
      const xdr = Buffer.from(xdrHex, 'hex');
      assert.strictEqual(xdr.length, 93);

      assert.strictEqual(polygon().toWKB().toString('hex'), xdr.toString('hex'));
      assert.isTrue(Geometry.fromWKB(xdr, 4326).equals(polygon()));
    });

    it('rejects the bytes PostGIS writes for ST_AsEWKB', () => {
      const ewkb = Buffer.from(ewkbHex, 'hex');
      assert.strictEqual(ewkb.length, 97);

      assert.throws(() => Geometry.fromWKB(ewkb, 4326), /EWKB/);
    });
  });

  describe('immutability', () => {
    it('rejects assignment to a field', () => {
      const point = Point.of(1, 2);
      assert.throws(() => {
        point.x = 5;
      }, TypeError);
      assert.throws(() => {
        point.extra = 5;
      }, TypeError);
    });

    it('rejects a change to a parts array', () => {
      const lineString = LineString.of(Point.of(0, 0), Point.of(1, 1));
      assert.throws(() => lineString.points.push(Point.of(2, 2)), TypeError);
      const polygon = polygonWithHole();
      assert.throws(() => polygon.interiorRings.pop(), TypeError);
      assert.throws(() => nestedCollection().geometries.push(Point.of(9, 9)), TypeError);
    });
  });

  describe('hierarchy', () => {
    it('mirrors the kinds of the Gremlin geometry type', () => {
      assert.instanceOf(Point.of(1, 2), Geometry);
      assert.instanceOf(LineString.empty(), Curve);
      assert.instanceOf(Polygon.empty(), Surface);
      assert.instanceOf(MultiLineString.empty(), MultiCurve);
      assert.instanceOf(MultiPolygon.empty(), MultiSurface);
      assert.instanceOf(MultiPoint.empty(), GeometryCollection);
      assert.instanceOf(MultiLineString.empty(), GeometryCollection);
      assert.instanceOf(MultiPolygon.empty(), GeometryCollection);
    });
  });
});
