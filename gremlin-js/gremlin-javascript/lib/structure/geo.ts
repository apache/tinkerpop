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

/**
 * Geospatial value types: a point, a line string, a polygon and the collections
 * built from them.
 *
 * Values are created through the static factories of each kind, are frozen, and
 * compare by value through {@link Geometry#equals}. Their string form is ISO WKT.
 */

import { Buffer } from 'buffer';

/** SRID of the {@link Model.CARTESIAN} model. */
const SRID_CARTESIAN = 0;

/** SRID of the {@link Model.GEOGRAPHIC} model, WGS84. */
const SRID_GEOGRAPHIC = 4326;

/** Deepest collection nesting a value may carry. */
const MAX_NESTING_DEPTH = 32;

const INT32_MIN = -2147483648;
const INT32_MAX = 2147483647;

const WKB_TYPE_POINT = 1;
const WKB_TYPE_LINESTRING = 2;
const WKB_TYPE_POLYGON = 3;
const WKB_TYPE_MULTIPOINT = 4;
const WKB_TYPE_MULTILINESTRING = 5;
const WKB_TYPE_MULTIPOLYGON = 6;
const WKB_TYPE_GEOMETRYCOLLECTION = 7;

/** Byte order marker of a big-endian WKB geometry. */
const WKB_BIG_ENDIAN = 0x00;

/** Byte order marker of a little-endian WKB geometry. */
const WKB_LITTLE_ENDIAN = 0x01;

/** EWKB flag bits, none of which ISO WKB uses. */
const EWKB_Z_FLAG = 0x80000000;
const EWKB_M_FLAG = 0x40000000;
const EWKB_SRID_FLAG = 0x20000000;

/**
 * The coordinate interpretation model of a geometry.
 *
 * `CARTESIAN` is planar, with math in coordinate units. `GEOGRAPHIC` is
 * longitude and latitude on the WGS84 earth. `OTHER` describes data in any
 * other coordinate system, such as a map projection.
 *
 * `OTHER` is only ever reported by {@link Geometry#model}. It is never accepted
 * by a factory, because a geometry in another coordinate system is meaningless
 * without knowing which one, and the SRID says exactly that.
 */
export const Model = Object.freeze({
  CARTESIAN: 'CARTESIAN',
  GEOGRAPHIC: 'GEOGRAPHIC',
  OTHER: 'OTHER',
} as const);

export type Model = (typeof Model)[keyof typeof Model];

/**
 * How a factory is told which coordinate system a geometry is in: either a
 * {@link Model} other than `OTHER`, or an SRID as an integer in the signed
 * 32-bit range.
 */
export type ModelOrSrid = Model | number;

/**
 * Returns the model an SRID stands for.
 */
function modelForSrid(srid: number): Model {
  if (srid === SRID_CARTESIAN) {
    return Model.CARTESIAN;
  }
  if (srid === SRID_GEOGRAPHIC) {
    return Model.GEOGRAPHIC;
  }
  return Model.OTHER;
}

/**
 * Returns the SRID a factory should use for the given model or SRID.
 */
function resolveSrid(modelOrSrid: ModelOrSrid): number {
  if (typeof modelOrSrid === 'number') {
    if (!Number.isInteger(modelOrSrid) || modelOrSrid < INT32_MIN || modelOrSrid > INT32_MAX) {
      throw new Error(`SRID must be an integer in the signed 32-bit range, got: ${modelOrSrid}`);
    }
    return modelOrSrid;
  }
  if (modelOrSrid === Model.CARTESIAN) {
    return SRID_CARTESIAN;
  }
  if (modelOrSrid === Model.GEOGRAPHIC) {
    return SRID_GEOGRAPHIC;
  }
  if (modelOrSrid === Model.OTHER) {
    throw new Error('Model.OTHER cannot be used in a factory; pass an explicit SRID instead');
  }
  throw new Error(`Model must be Model.CARTESIAN or Model.GEOGRAPHIC, or an SRID number, got: ${String(modelOrSrid)}`);
}

function validateFiniteNumber(value: number, name: string): void {
  if (typeof value !== 'number') {
    throw new Error(`${name} coordinate must be a number, got: ${typeof value}`);
  }
  if (!Number.isFinite(value)) {
    throw new Error(`${name} coordinate cannot be NaN or infinite`);
  }
}

function validateX(x: number, srid: number): void {
  validateFiniteNumber(x, 'x');
  if (srid === SRID_GEOGRAPHIC && (x < -180 || x > 180)) {
    throw new Error(`GEOGRAPHIC longitude must be in [-180, 180], got: ${x}`);
  }
}

function validateY(y: number, srid: number): void {
  validateFiniteNumber(y, 'y');
  if (srid === SRID_GEOGRAPHIC && (y < -90 || y > 90)) {
    throw new Error(`GEOGRAPHIC latitude must be in [-90, 90], got: ${y}`);
  }
}

function validateZ(z: number): void {
  if (typeof z !== 'number') {
    throw new Error(`z coordinate must be a number, got: ${typeof z}`);
  }
  if (Number.isNaN(z)) {
    throw new Error('z coordinate cannot be NaN; use Point.of for a 2D point');
  }
  if (!Number.isFinite(z)) {
    throw new Error('z coordinate cannot be infinite');
  }
}

function validateSameSrid(expected: number, actual: number, context: string): void {
  if (expected !== actual) {
    throw new Error(`Mixed SRIDs in ${context}: expected ${expected}, got ${actual}`);
  }
}

function validateSameHasZ(expected: boolean, actual: boolean, context: string): void {
  if (expected !== actual) {
    throw new Error(`Mixed z dimensions in ${context}: some coordinates have z, some do not`);
  }
}

function validateNestingDepth(depth: number): void {
  if (depth > MAX_NESTING_DEPTH) {
    throw new Error(`Collection nesting depth exceeds maximum of ${MAX_NESTING_DEPTH}`);
  }
}

/**
 * Returns the collection nesting depth of a geometry. Every collection,
 * including a multi-kind, counts as one level plus its deepest part.
 */
function computeDepth(geometry: Geometry): number {
  if (!(geometry instanceof GeometryCollection)) {
    return 0;
  }
  let deepestPart = 0;
  for (const part of geometry.geometries) {
    deepestPart = Math.max(deepestPart, computeDepth(part));
  }
  return 1 + deepestPart;
}

/**
 * Checks a ring of a polygon: an empty ring stands for an empty polygon, and
 * any other ring must be closed and hold at least 4 points.
 */
function validateRing(ring: LineString, context: string): void {
  if (ring.isEmpty) {
    return;
  }
  const points = ring.points;
  if (points.length < 4) {
    throw new Error(`${context} must have at least 4 points, got: ${points.length}`);
  }
  if (!points[0].equals(points[points.length - 1])) {
    throw new Error(`${context} must be closed (first and last point must be equal)`);
  }
}

/**
 * Checks a point used as a vertex of a line string or a polygon ring. A vertex
 * is a real coordinate, so a value of another kind and an empty point are both
 * rejected.
 */
function validateVertex(point: Point, context: string, index: number): void {
  requireNonNullElement(point, context, index);
  requireElementOfKind(point, POINT_PARTS, context, index);
  if (point.isEmpty) {
    throw new Error(`${context} vertex at index ${index} cannot be an empty point; a vertex must be a real coordinate`);
  }
}

function requireNonNullElement(element: unknown, context: string, index: number): void {
  if (element === null || element === undefined) {
    throw new Error(`${context} element at index ${index} cannot be null`);
  }
}

/**
 * The kind of part a factory admits, in the words its message uses.
 */
type PartKind = {
  /** The plural noun for the parts, as `polygons`. */
  readonly noun: string;
  /** The name of the class a part must be, as `Polygon`. */
  readonly name: string;
  /** Whether a value is of the kind. */
  readonly matches: (value: unknown) => boolean;
};

const POINT_PARTS: PartKind = {
  noun: 'points',
  name: 'Point',
  matches: (value) => value instanceof Point,
};

const LINE_STRING_PARTS: PartKind = {
  noun: 'line strings',
  name: 'LineString',
  matches: (value) => value instanceof LineString,
};

const POLYGON_PARTS: PartKind = {
  noun: 'polygons',
  name: 'Polygon',
  matches: (value) => value instanceof Polygon,
};

const GEOMETRY_PARTS: PartKind = {
  noun: 'geometries',
  name: 'Geometry',
  matches: (value) => value instanceof Geometry,
};

/**
 * Names the kind of a value for a message: the class of an object, and the type
 * of anything else.
 */
function describeKind(value: unknown): string {
  if (typeof value === 'object' && value !== null) {
    return value.constructor?.name ?? 'Object';
  }
  return typeof value;
}

/**
 * Checks that a part is of the kind the factory admits. The parameter types say
 * the same, and hold only where the caller is compiled against them.
 */
function requireElementOfKind(element: unknown, kind: PartKind, context: string, index: number): void {
  if (!kind.matches(element)) {
    throw new Error(
      `${context} ${kind.noun} element at index ${index} must be a ${kind.name}, got: ${describeKind(element)}`,
    );
  }
}

function geometriesEqual(left: readonly Geometry[], right: readonly Geometry[]): boolean {
  if (left.length !== right.length) {
    return false;
  }
  for (let i = 0; i < left.length; i++) {
    if (!left[i].equals(right[i])) {
      return false;
    }
  }
  return true;
}

/**
 * Formats a coordinate as the shortest decimal that reads back as the same
 * number, in plain notation with no exponent and no trailing zeros. Negative
 * zero keeps its sign.
 */
function formatCoordinate(value: number): string {
  if (!Number.isFinite(value)) {
    throw new Error('Cannot format NaN or infinity in WKT');
  }
  if (value === 0) {
    return Object.is(value, -0) ? '-0' : '0';
  }
  const text = String(value);
  const exponentAt = text.indexOf('e');
  return exponentAt === -1 ? text : toPlainDecimal(text, exponentAt);
}

/**
 * Rewrites the exponent form that `Number.prototype.toString()` gives for
 * magnitudes of 1e21 and above and below 1e-6 as a plain decimal.
 */
function toPlainDecimal(text: string, exponentAt: number): string {
  const sign = text.charAt(0) === '-' ? '-' : '';
  const mantissa = text.substring(sign.length, exponentAt);
  const exponent = Number(text.substring(exponentAt + 1));
  const pointAt = mantissa.indexOf('.');
  const digits = pointAt === -1 ? mantissa : mantissa.substring(0, pointAt) + mantissa.substring(pointAt + 1);
  const fractionLength = pointAt === -1 ? 0 : mantissa.length - pointAt - 1;
  // the value is digits x 10^scale
  const scale = exponent - fractionLength;
  if (scale >= 0) {
    return sign + digits + '0'.repeat(scale);
  }
  // a negative exponent only appears for a magnitude below 1e-6, where the
  // exponent is -7 or less and the mantissa carries one digit before the point,
  // so the decimal point always falls to the left of every digit
  return sign + '0.' + '0'.repeat(-scale - digits.length) + digits;
}

/**
 * Appends a geometry with its type name, its Z tag and either the EMPTY keyword
 * or a parenthesized body. The choice is structural, so the WKT and the WKB of
 * one value describe the same parts.
 */
function writeWktTagged(out: string[], geometry: Geometry): void {
  out.push(geometry._wktTypeName());
  if (geometry.hasZ) {
    out.push(' Z');
  }
  if (geometry._hasNoParts()) {
    out.push(' EMPTY');
    return;
  }
  if (geometry.hasZ) {
    out.push(' ');
  }
  out.push('(');
  geometry._writeWktBody(out);
  out.push(')');
}

function wkbInt32(value: number): Buffer {
  const buffer = Buffer.alloc(4);
  buffer.writeInt32BE(value, 0);
  return buffer;
}

function wkbDouble(value: number): Buffer {
  const buffer = Buffer.alloc(8);
  buffer.writeDoubleBE(value, 0);
  return buffer;
}

/**
 * Appends the byte order marker, the ISO type code and the body of a geometry,
 * in big-endian byte order.
 */
function writeWkbGeometry(parts: Buffer[], geometry: Geometry): void {
  parts.push(Buffer.from([WKB_BIG_ENDIAN]));
  parts.push(wkbInt32(geometry._wkbTypeCode() + (geometry.hasZ ? 1000 : 0)));
  geometry._writeWkbBody(parts);
}

/**
 * Appends the coordinates of a point, with z included when the enclosing
 * geometry is 3D. ISO WKB encodes an empty point as NaN coordinates.
 */
function writeWkbCoordinate(parts: Buffer[], point: Point, hasZ: boolean): void {
  if (point.isEmpty) {
    parts.push(wkbDouble(Number.NaN));
    parts.push(wkbDouble(Number.NaN));
    if (hasZ) {
      parts.push(wkbDouble(Number.NaN));
    }
    return;
  }
  parts.push(wkbDouble(point.x));
  parts.push(wkbDouble(point.y));
  if (hasZ) {
    parts.push(wkbDouble(point.z as number));
  }
}

/**
 * Base class of every geometry kind. Geometry values are frozen.
 */
export abstract class Geometry {
  #srid: number;
  #hasZ: boolean;

  protected constructor(srid: number, hasZ: boolean) {
    this.#srid = srid;
    this.#hasZ = hasZ;
  }

  /**
   * Parses one ISO WKB geometry from a byte array, with the given SRID.
   *
   * ISO WKB only: EWKB, the M and ZM dimensions and unknown type codes are
   * rejected. Each geometry header carries its own byte order marker, so the
   * input and any geometry nested in it may be little-endian. The array must
   * hold exactly one geometry, so input that ends inside the geometry and bytes
   * that follow it are both rejected.
   *
   * @param wkb the ISO WKB bytes
   * @param srid the spatial reference ID, applied as given and not itself
   *        validated
   * @throws {Error} if `wkb` is not a byte array or is empty, if the bytes are
   *         malformed or end inside the geometry, or if bytes follow the
   *         geometry
   */
  static fromWKB(wkb: Uint8Array, srid: number): Geometry {
    if (!(wkb instanceof Uint8Array)) {
      throw new Error(`WKB input must be a Uint8Array, got: ${typeof wkb}`);
    }
    if (wkb.length === 0) {
      throw new Error('WKB input cannot be empty');
    }
    const bytes = new WkbBytes(wkb);
    const parse = new WkbParser(srid).readGeometry();
    let step = parse.next();
    while (!step.done) {
      step = parse.next(bytes.read(step.value));
    }
    if (bytes.remaining > 0) {
      throw new Error('Unexpected trailing bytes in WKB input');
    }
    return step.value;
  }

  /**
   * The spatial reference ID: 0 for `CARTESIAN`, 4326 for `GEOGRAPHIC`, or any
   * other coordinate system.
   */
  get srid(): number {
    return this.#srid;
  }

  /**
   * The coordinate interpretation model the SRID stands for.
   */
  get model(): Model {
    return modelForSrid(this.#srid);
  }

  /**
   * Whether this geometry carries z coordinates.
   */
  get hasZ(): boolean {
    return this.#hasZ;
  }

  /**
   * Whether this geometry holds no coordinates. A collection is empty when all
   * of its parts are empty.
   */
  abstract get isEmpty(): boolean;

  /**
   * Returns ISO WKT without an SRID prefix.
   */
  toWKT(): string {
    const out: string[] = [];
    writeWktTagged(out, this);
    return out.join('');
  }

  /**
   * Returns EWKT, the WKT with an `SRID=n;` prefix.
   */
  toEWKT(): string {
    return `SRID=${this.#srid};${this.toWKT()}`;
  }

  /**
   * Returns ISO WKB in big-endian byte order.
   */
  toWKB(): Buffer {
    const parts: Buffer[] = [];
    writeWkbGeometry(parts, this);
    return Buffer.concat(parts);
  }

  /**
   * Compares by value: same kind, same SRID, same z dimension and same
   * coordinates, where -0 equals 0 and two empty points of one kind, SRID and z
   * dimension are equal.
   */
  equals(other: unknown): boolean {
    if (this === other) {
      return true;
    }
    if (!(other instanceof Geometry)) {
      return false;
    }
    if (this.constructor !== other.constructor) {
      return false;
    }
    if (this._wkbTypeCode() !== other._wkbTypeCode()) {
      return false;
    }
    if (this.#srid !== other.#srid || this.#hasZ !== other.#hasZ) {
      return false;
    }
    return this._partsEqual(other);
  }

  /**
   * Returns the WKT, as {@link Geometry#toWKT} gives it.
   */
  toString(): string {
    return this.toWKT();
  }

  /** @internal The WKB type code of this kind, which also identifies the kind. */
  abstract _wkbTypeCode(): number;

  /** @internal The WKT type name of this kind. */
  abstract _wktTypeName(): string;

  /**
   * @internal Whether this geometry holds no parts of its own: no coordinate for
   * a point, no points for a line string, no rings for a polygon, no parts for a
   * collection. This is what the writers test to choose between `X EMPTY` and a
   * parenthesized body, and it differs from {@link Geometry#isEmpty}, which is
   * deep.
   */
  abstract _hasNoParts(): boolean;

  /** @internal Appends the WKT coordinates or parts of this geometry. */
  abstract _writeWktBody(out: string[]): void;

  /** @internal Appends the WKB coordinates or parts of this geometry. */
  abstract _writeWkbBody(parts: Buffer[]): void;

  /** @internal Compares the parts, once the kind, SRID and z dimension match. */
  abstract _partsEqual(other: Geometry): boolean;
}

/**
 * Base class of the one-dimensional geometry kinds. The only kind is
 * {@link LineString}.
 */
export abstract class Curve extends Geometry {
  protected constructor(srid: number, hasZ: boolean) {
    super(srid, hasZ);
  }
}

/**
 * Base class of the two-dimensional geometry kinds. The only kind is
 * {@link Polygon}.
 */
export abstract class Surface extends Geometry {
  protected constructor(srid: number, hasZ: boolean) {
    super(srid, hasZ);
  }
}

/**
 * A single location in 2D or 3D space.
 */
export class Point extends Geometry {
  #x: number | undefined;
  #y: number | undefined;
  #z: number | undefined;

  private constructor(
    x: number | undefined,
    y: number | undefined,
    z: number | undefined,
    srid: number,
    hasZ: boolean,
  ) {
    super(srid, hasZ);
    this.#x = x;
    this.#y = y;
    this.#z = z;
    Object.freeze(this);
  }

  /**
   * Creates a 2D point.
   *
   * @param x the x coordinate, the longitude under `GEOGRAPHIC`
   * @param y the y coordinate, the latitude under `GEOGRAPHIC`
   * @param modelOrSrid `Model.CARTESIAN`, `Model.GEOGRAPHIC` or an SRID
   * @throws {Error} if a coordinate is not a number, is NaN or is infinite, if
   *         the SRID is 4326 and x is outside -180 to 180 or y is outside -90 to
   *         90, or if `modelOrSrid` is `Model.OTHER` or not a usable SRID
   */
  static of(x: number, y: number, modelOrSrid: ModelOrSrid = Model.CARTESIAN): Point {
    const srid = resolveSrid(modelOrSrid);
    validateX(x, srid);
    validateY(y, srid);
    return new Point(x, y, undefined, srid, false);
  }

  /**
   * Creates a 3D point.
   *
   * @param x the x coordinate, the longitude under `GEOGRAPHIC`
   * @param y the y coordinate, the latitude under `GEOGRAPHIC`
   * @param z the z coordinate
   * @param modelOrSrid `Model.CARTESIAN`, `Model.GEOGRAPHIC` or an SRID
   * @throws {Error} if a coordinate is not a number, is NaN or is infinite, if
   *         the SRID is 4326 and x is outside -180 to 180 or y is outside -90 to
   *         90, or if `modelOrSrid` is `Model.OTHER` or not a usable SRID
   */
  static ofZ(x: number, y: number, z: number, modelOrSrid: ModelOrSrid = Model.CARTESIAN): Point {
    const srid = resolveSrid(modelOrSrid);
    validateX(x, srid);
    validateY(y, srid);
    validateZ(z);
    return new Point(x, y, z, srid, true);
  }

  /**
   * Creates an empty 2D point, which holds no coordinate.
   *
   * @param modelOrSrid `Model.CARTESIAN`, `Model.GEOGRAPHIC` or an SRID
   * @throws {Error} if `modelOrSrid` is `Model.OTHER` or not a usable SRID
   */
  static empty(modelOrSrid: ModelOrSrid = Model.CARTESIAN): Point {
    return new Point(undefined, undefined, undefined, resolveSrid(modelOrSrid), false);
  }

  /** @internal Creates an empty point with an explicit z dimension. */
  static _emptyWithZ(srid: number, hasZ: boolean): Point {
    return new Point(undefined, undefined, undefined, srid, hasZ);
  }

  /** @internal Creates a point with an explicit SRID, for the WKB reader. */
  static _ofWithSrid(x: number, y: number, z: number | undefined, srid: number): Point {
    validateX(x, srid);
    validateY(y, srid);
    if (z === undefined) {
      return new Point(x, y, undefined, srid, false);
    }
    validateZ(z);
    return new Point(x, y, z, srid, true);
  }

  /**
   * The x coordinate, the longitude under `GEOGRAPHIC`.
   *
   * @throws {Error} if this point is empty
   */
  get x(): number {
    if (this.#x === undefined) {
      throw new Error('Cannot get x from an empty point');
    }
    return this.#x;
  }

  /**
   * The y coordinate, the latitude under `GEOGRAPHIC`.
   *
   * @throws {Error} if this point is empty
   */
  get y(): number {
    if (this.#y === undefined) {
      throw new Error('Cannot get y from an empty point');
    }
    return this.#y;
  }

  /**
   * The z coordinate, or `undefined` for a 2D point.
   *
   * @throws {Error} if this point is empty
   */
  get z(): number | undefined {
    if (this.isEmpty) {
      throw new Error('Cannot get z from an empty point');
    }
    return this.#z;
  }

  get isEmpty(): boolean {
    return this.#x === undefined;
  }

  /** @internal */
  _wkbTypeCode(): number {
    return WKB_TYPE_POINT;
  }

  /** @internal */
  _wktTypeName(): string {
    return 'POINT';
  }

  /** @internal */
  _hasNoParts(): boolean {
    return this.#x === undefined;
  }

  /** @internal */
  _writeWktBody(out: string[]): void {
    if (this.#x === undefined || this.#y === undefined) {
      return;
    }
    out.push(formatCoordinate(this.#x));
    out.push(' ');
    out.push(formatCoordinate(this.#y));
    if (this.hasZ && this.#z !== undefined) {
      out.push(' ');
      out.push(formatCoordinate(this.#z));
    }
  }

  /** @internal */
  _writeWkbBody(parts: Buffer[]): void {
    writeWkbCoordinate(parts, this, this.hasZ);
  }

  /** @internal */
  _partsEqual(other: Geometry): boolean {
    const point = other as Point;
    if (this.isEmpty !== point.isEmpty) {
      return false;
    }
    if (this.isEmpty) {
      return true;
    }
    if (this.#x !== point.#x || this.#y !== point.#y) {
      return false;
    }
    return !this.hasZ || this.#z === point.#z;
  }
}

/**
 * A sequence of connected line segments.
 */
export class LineString extends Curve {
  #points: readonly Point[];

  private constructor(points: readonly Point[], srid: number, hasZ: boolean) {
    super(srid, hasZ);
    this.#points = Object.freeze(points);
    Object.freeze(this);
  }

  /**
   * Creates a line string from at least 2 points, which must share an SRID and
   * a z dimension and none of which may be empty.
   *
   * @throws {Error} if fewer than 2 points are given, if a point is null, is not
   *         a point or is empty, or if the points do not share an SRID and a z
   *         dimension
   */
  static of(...points: Point[]): LineString {
    if (points.length === 0) {
      throw new Error('LineString must have at least 2 points, got 0; use LineString.empty() for an empty line string');
    }
    if (points.length === 1) {
      throw new Error('LineString must have at least 2 points, got 1');
    }

    validateVertex(points[0], 'LineString', 0);
    const srid = points[0].srid;
    const hasZ = points[0].hasZ;
    for (let i = 1; i < points.length; i++) {
      validateVertex(points[i], 'LineString', i);
      validateSameSrid(srid, points[i].srid, 'LineString');
      validateSameHasZ(hasZ, points[i].hasZ, 'LineString');
    }

    return new LineString(points, srid, hasZ);
  }

  /**
   * Creates an empty line string, which holds no points.
   *
   * @param modelOrSrid `Model.CARTESIAN`, `Model.GEOGRAPHIC` or an SRID
   * @throws {Error} if `modelOrSrid` is `Model.OTHER` or not a usable SRID
   */
  static empty(modelOrSrid: ModelOrSrid = Model.CARTESIAN): LineString {
    return new LineString([], resolveSrid(modelOrSrid), false);
  }

  /** @internal Creates a line string with an explicit SRID, for the WKB reader. */
  static _ofWithSrid(points: Point[], srid: number, hasZ: boolean): LineString {
    if (points.length === 0) {
      return new LineString([], srid, hasZ);
    }
    if (points.length === 1) {
      throw new Error('LineString must have at least 2 points, got 1');
    }
    for (let i = 0; i < points.length; i++) {
      validateVertex(points[i], 'LineString', i);
      validateSameSrid(srid, points[i].srid, 'LineString');
      validateSameHasZ(hasZ, points[i].hasZ, 'LineString');
    }
    return new LineString(points, srid, hasZ);
  }

  /**
   * The points of this line string, an empty array when it is empty.
   */
  get points(): readonly Point[] {
    return this.#points;
  }

  get isEmpty(): boolean {
    return this.#points.length === 0;
  }

  /** @internal */
  _wkbTypeCode(): number {
    return WKB_TYPE_LINESTRING;
  }

  /** @internal */
  _wktTypeName(): string {
    return 'LINESTRING';
  }

  /** @internal */
  _hasNoParts(): boolean {
    return this.#points.length === 0;
  }

  /** @internal */
  _writeWktBody(out: string[]): void {
    for (let i = 0; i < this.#points.length; i++) {
      if (i > 0) {
        out.push(', ');
      }
      this.#points[i]._writeWktBody(out);
    }
  }

  /** @internal */
  _writeWkbBody(parts: Buffer[]): void {
    parts.push(wkbInt32(this.#points.length));
    for (const point of this.#points) {
      writeWkbCoordinate(parts, point, this.hasZ);
    }
  }

  /** @internal */
  _partsEqual(other: Geometry): boolean {
    return geometriesEqual(this.#points, (other as LineString).#points);
  }
}

/**
 * An area enclosed by a shell ring, with any number of hole rings. Every ring
 * is closed and holds at least 4 points.
 */
export class Polygon extends Surface {
  #shell: LineString;
  #holes: readonly LineString[];

  private constructor(shell: LineString, holes: readonly LineString[], srid: number, hasZ: boolean) {
    super(srid, hasZ);
    this.#shell = shell;
    this.#holes = Object.freeze(holes);
    Object.freeze(this);
  }

  /**
   * Creates a polygon from a shell ring and any number of hole rings, which
   * must share an SRID and a z dimension.
   *
   * @throws {Error} if the shell is not a line string, if a ring that is not
   *         empty holds fewer than 4 points or is not closed, if a hole is null
   *         or empty, if the shell is empty and any hole is given, or if the
   *         rings do not share an SRID and a z dimension
   */
  static of(shell: LineString, ...holes: LineString[]): Polygon {
    if (shell === null || shell === undefined) {
      throw new Error('Shell ring cannot be null');
    }
    if (!(shell instanceof LineString)) {
      throw new Error(`Shell ring must be a LineString, got: ${describeKind(shell)}`);
    }
    validateRing(shell, 'Shell ring');
    const srid = shell.srid;
    const hasZ = shell.hasZ;
    validateHoles(shell, holes, srid, hasZ);
    return new Polygon(shell, holes, srid, hasZ);
  }

  /**
   * Creates an empty polygon, which holds no rings.
   *
   * @param modelOrSrid `Model.CARTESIAN`, `Model.GEOGRAPHIC` or an SRID
   * @throws {Error} if `modelOrSrid` is `Model.OTHER` or not a usable SRID
   */
  static empty(modelOrSrid: ModelOrSrid = Model.CARTESIAN): Polygon {
    const srid = resolveSrid(modelOrSrid);
    return new Polygon(LineString.empty(srid), [], srid, false);
  }

  /** @internal Creates a polygon with an explicit SRID, for the WKB reader. */
  static _ofWithSrid(shell: LineString, holes: LineString[], srid: number, hasZ: boolean): Polygon {
    if (shell.isEmpty && holes.length === 0) {
      return new Polygon(LineString._ofWithSrid([], srid, hasZ), [], srid, hasZ);
    }
    if (!shell.isEmpty) {
      validateRing(shell, 'Shell ring');
      validateSameSrid(srid, shell.srid, 'Polygon');
      validateSameHasZ(hasZ, shell.hasZ, 'Polygon');
    }
    validateHoles(shell, holes, srid, hasZ);
    return new Polygon(shell, holes, srid, hasZ);
  }

  /**
   * The exterior ring, an empty line string when this polygon is empty.
   */
  get exteriorRing(): LineString {
    return this.#shell;
  }

  /**
   * The interior rings, an empty array when this polygon has no holes.
   */
  get interiorRings(): readonly LineString[] {
    return this.#holes;
  }

  get isEmpty(): boolean {
    return this.#shell.isEmpty;
  }

  /** @internal */
  _wkbTypeCode(): number {
    return WKB_TYPE_POLYGON;
  }

  /** @internal */
  _wktTypeName(): string {
    return 'POLYGON';
  }

  /** @internal */
  _hasNoParts(): boolean {
    return this.#shell._hasNoParts();
  }

  /** @internal */
  _writeWktBody(out: string[]): void {
    if (this.#shell._hasNoParts()) {
      return;
    }
    out.push('(');
    this.#shell._writeWktBody(out);
    out.push(')');
    for (const hole of this.#holes) {
      out.push(', (');
      hole._writeWktBody(out);
      out.push(')');
    }
  }

  /** @internal */
  _writeWkbBody(parts: Buffer[]): void {
    const ringCount = this.#shell._hasNoParts() ? 0 : 1 + this.#holes.length;
    parts.push(wkbInt32(ringCount));
    if (ringCount === 0) {
      return;
    }
    this.#shell._writeWkbBody(parts);
    for (const hole of this.#holes) {
      hole._writeWkbBody(parts);
    }
  }

  /** @internal */
  _partsEqual(other: Geometry): boolean {
    const polygon = other as Polygon;
    return this.#shell.equals(polygon.#shell) && geometriesEqual(this.#holes, polygon.#holes);
  }
}

/**
 * Checks the hole rings of a polygon against its shell.
 */
function validateHoles(shell: LineString, holes: readonly LineString[], srid: number, hasZ: boolean): void {
  for (let i = 0; i < holes.length; i++) {
    const hole = holes[i];
    requireNonNullElement(hole, 'Polygon holes', i);
    if (hole.isEmpty) {
      throw new Error(`Hole ring at index ${i} cannot be empty`);
    }
    if (shell.isEmpty) {
      throw new Error('Polygon with an empty shell cannot have holes');
    }
    validateSameSrid(srid, hole.srid, 'Polygon');
    validateSameHasZ(hasZ, hole.hasZ, 'Polygon');
    validateRing(hole, 'Hole ring');
  }
}

/**
 * Any mix of geometry kinds held together.
 */
export class GeometryCollection extends Geometry {
  #geometries: readonly Geometry[];

  protected constructor(geometries: readonly Geometry[], srid: number, hasZ: boolean) {
    super(srid, hasZ);
    this.#geometries = Object.freeze(geometries);
    // the multi-kinds keep their parts in this field and declare none of their
    // own, so a value is complete once this constructor has run
    Object.freeze(this);
  }

  /**
   * Creates a collection from at least 1 geometry, which must share an SRID and
   * a z dimension. A part may be empty, and may itself be a collection as long
   * as the nesting stays within 32 levels.
   *
   * @throws {Error} if no geometry is given, if a part is null or is not a
   *         geometry, if the parts do not share an SRID and a z dimension, or if
   *         the nesting would exceed 32 levels
   */
  static of(...geometries: Geometry[]): GeometryCollection {
    const checked = validateCollectionParts(geometries, 'GeometryCollection', GEOMETRY_PARTS, true);
    return new GeometryCollection(geometries, checked.srid, checked.hasZ);
  }

  /**
   * Creates an empty collection, which holds no parts.
   *
   * @param modelOrSrid `Model.CARTESIAN`, `Model.GEOGRAPHIC` or an SRID
   * @throws {Error} if `modelOrSrid` is `Model.OTHER` or not a usable SRID
   */
  static empty(modelOrSrid: ModelOrSrid = Model.CARTESIAN): GeometryCollection {
    return new GeometryCollection([], resolveSrid(modelOrSrid), false);
  }

  /** @internal Creates a collection with an explicit SRID, for the WKB reader. */
  static _ofWithSrid(geometries: Geometry[], srid: number, hasZ: boolean): GeometryCollection {
    validatePartsAgainst(geometries, srid, hasZ, 'GeometryCollection');
    return new GeometryCollection(geometries, srid, hasZ);
  }

  /**
   * The parts of this collection, an empty array when it holds none.
   */
  get geometries(): readonly Geometry[] {
    return this.#geometries;
  }

  get isEmpty(): boolean {
    for (const part of this.#geometries) {
      if (!part.isEmpty) {
        return false;
      }
    }
    return true;
  }

  /** @internal */
  _wkbTypeCode(): number {
    return WKB_TYPE_GEOMETRYCOLLECTION;
  }

  /** @internal */
  _wktTypeName(): string {
    return 'GEOMETRYCOLLECTION';
  }

  /** @internal */
  _hasNoParts(): boolean {
    return this.#geometries.length === 0;
  }

  /** @internal */
  _writeWktBody(out: string[]): void {
    for (let i = 0; i < this.#geometries.length; i++) {
      if (i > 0) {
        out.push(', ');
      }
      writeWktTagged(out, this.#geometries[i]);
    }
  }

  /** @internal */
  _writeWkbBody(parts: Buffer[]): void {
    parts.push(wkbInt32(this.#geometries.length));
    for (const part of this.#geometries) {
      writeWkbGeometry(parts, part);
    }
  }

  /** @internal */
  _partsEqual(other: Geometry): boolean {
    return geometriesEqual(this.#geometries, (other as GeometryCollection).#geometries);
  }

  /**
   * @internal Appends the WKT body of a multi-kind, where a part that holds no
   * parts of its own is written as the EMPTY keyword rather than a tagged
   * geometry.
   */
  _writeMultiWktBody(out: string[]): void {
    for (let i = 0; i < this.#geometries.length; i++) {
      if (i > 0) {
        out.push(', ');
      }
      const part = this.#geometries[i];
      if (part._hasNoParts()) {
        out.push('EMPTY');
      } else {
        out.push('(');
        part._writeWktBody(out);
        out.push(')');
      }
    }
  }
}

/**
 * Base class of the collections of curves. The only kind is
 * {@link MultiLineString}.
 */
export abstract class MultiCurve extends GeometryCollection {
  protected constructor(geometries: readonly Geometry[], srid: number, hasZ: boolean) {
    super(geometries, srid, hasZ);
  }
}

/**
 * Base class of the collections of surfaces. The only kind is
 * {@link MultiPolygon}.
 */
export abstract class MultiSurface extends GeometryCollection {
  protected constructor(geometries: readonly Geometry[], srid: number, hasZ: boolean) {
    super(geometries, srid, hasZ);
  }
}

/**
 * Checks the parts of a collection and returns the SRID and z dimension they
 * share.
 */
function validateCollectionParts(
  parts: readonly Geometry[],
  context: string,
  kind: PartKind,
  checkNesting: boolean,
): { srid: number; hasZ: boolean } {
  if (parts.length === 0) {
    throw new Error(`${context} must have at least 1 part; use ${context}.empty() for an empty value`);
  }
  requireNonNullElement(parts[0], context, 0);
  requireElementOfKind(parts[0], kind, context, 0);
  const srid = parts[0].srid;
  const hasZ = parts[0].hasZ;
  let deepestPart = 0;
  for (let i = 0; i < parts.length; i++) {
    requireNonNullElement(parts[i], context, i);
    requireElementOfKind(parts[i], kind, context, i);
    validateSameSrid(srid, parts[i].srid, context);
    validateSameHasZ(hasZ, parts[i].hasZ, context);
    if (checkNesting) {
      deepestPart = Math.max(deepestPart, computeDepth(parts[i]));
    }
  }
  if (checkNesting) {
    // this level plus the deepest part must stay within the nesting limit
    validateNestingDepth(1 + deepestPart);
  }
  return { srid, hasZ };
}

/**
 * Checks the parts read from WKB against the SRID and z dimension of the value
 * that holds them.
 */
function validatePartsAgainst(parts: readonly Geometry[], srid: number, hasZ: boolean, context: string): void {
  for (const part of parts) {
    validateSameSrid(srid, part.srid, context);
    validateSameHasZ(hasZ, part.hasZ, context);
  }
}

/**
 * Any number of points held together.
 */
export class MultiPoint extends GeometryCollection {
  /**
   * Creates a multi-point from at least 1 point, which must share an SRID and a
   * z dimension. A part may be an empty point.
   *
   * @throws {Error} if no point is given, if a point is null or is not a point,
   *         or if the points do not share an SRID and a z dimension
   */
  static of(...points: Point[]): MultiPoint {
    const checked = validateCollectionParts(points, 'MultiPoint', POINT_PARTS, false);
    return new MultiPoint(points, checked.srid, checked.hasZ);
  }

  /**
   * Creates an empty multi-point, which holds no points.
   *
   * @param modelOrSrid `Model.CARTESIAN`, `Model.GEOGRAPHIC` or an SRID
   * @throws {Error} if `modelOrSrid` is `Model.OTHER` or not a usable SRID
   */
  static empty(modelOrSrid: ModelOrSrid = Model.CARTESIAN): MultiPoint {
    return new MultiPoint([], resolveSrid(modelOrSrid), false);
  }

  /** @internal Creates a multi-point with an explicit SRID, for the WKB reader. */
  static _ofWithSrid(points: Point[], srid: number, hasZ: boolean): MultiPoint {
    validatePartsAgainst(points, srid, hasZ, 'MultiPoint');
    return new MultiPoint(points, srid, hasZ);
  }

  get geometries(): readonly Point[] {
    return super.geometries as readonly Point[];
  }

  /** @internal */
  _wkbTypeCode(): number {
    return WKB_TYPE_MULTIPOINT;
  }

  /** @internal */
  _wktTypeName(): string {
    return 'MULTIPOINT';
  }

  /** @internal */
  _writeWktBody(out: string[]): void {
    this._writeMultiWktBody(out);
  }
}

/**
 * Any number of line strings held together.
 */
export class MultiLineString extends MultiCurve {
  /**
   * Creates a multi-line string from at least 1 line string, which must share an
   * SRID and a z dimension. A part may be an empty line string.
   *
   * @throws {Error} if no line string is given, if a line string is null or is
   *         not a line string, or if the line strings do not share an SRID and a
   *         z dimension
   */
  static of(...lineStrings: LineString[]): MultiLineString {
    const checked = validateCollectionParts(lineStrings, 'MultiLineString', LINE_STRING_PARTS, false);
    return new MultiLineString(lineStrings, checked.srid, checked.hasZ);
  }

  /**
   * Creates an empty multi-line string, which holds no line strings.
   *
   * @param modelOrSrid `Model.CARTESIAN`, `Model.GEOGRAPHIC` or an SRID
   * @throws {Error} if `modelOrSrid` is `Model.OTHER` or not a usable SRID
   */
  static empty(modelOrSrid: ModelOrSrid = Model.CARTESIAN): MultiLineString {
    return new MultiLineString([], resolveSrid(modelOrSrid), false);
  }

  /** @internal Creates a multi-line string with an explicit SRID, for the WKB reader. */
  static _ofWithSrid(lineStrings: LineString[], srid: number, hasZ: boolean): MultiLineString {
    validatePartsAgainst(lineStrings, srid, hasZ, 'MultiLineString');
    return new MultiLineString(lineStrings, srid, hasZ);
  }

  get geometries(): readonly LineString[] {
    return super.geometries as readonly LineString[];
  }

  /** @internal */
  _wkbTypeCode(): number {
    return WKB_TYPE_MULTILINESTRING;
  }

  /** @internal */
  _wktTypeName(): string {
    return 'MULTILINESTRING';
  }

  /** @internal */
  _writeWktBody(out: string[]): void {
    this._writeMultiWktBody(out);
  }
}

/**
 * Any number of polygons held together.
 */
export class MultiPolygon extends MultiSurface {
  /**
   * Creates a multi-polygon from at least 1 polygon, which must share an SRID
   * and a z dimension. A part may be an empty polygon.
   *
   * @throws {Error} if no polygon is given, if a polygon is null or is not a
   *         polygon, or if the polygons do not share an SRID and a z dimension
   */
  static of(...polygons: Polygon[]): MultiPolygon {
    const checked = validateCollectionParts(polygons, 'MultiPolygon', POLYGON_PARTS, false);
    return new MultiPolygon(polygons, checked.srid, checked.hasZ);
  }

  /**
   * Creates an empty multi-polygon, which holds no polygons.
   *
   * @param modelOrSrid `Model.CARTESIAN`, `Model.GEOGRAPHIC` or an SRID
   * @throws {Error} if `modelOrSrid` is `Model.OTHER` or not a usable SRID
   */
  static empty(modelOrSrid: ModelOrSrid = Model.CARTESIAN): MultiPolygon {
    return new MultiPolygon([], resolveSrid(modelOrSrid), false);
  }

  /** @internal Creates a multi-polygon with an explicit SRID, for the WKB reader. */
  static _ofWithSrid(polygons: Polygon[], srid: number, hasZ: boolean): MultiPolygon {
    validatePartsAgainst(polygons, srid, hasZ, 'MultiPolygon');
    return new MultiPolygon(polygons, srid, hasZ);
  }

  get geometries(): readonly Polygon[] {
    return super.geometries as readonly Polygon[];
  }

  /** @internal */
  _wkbTypeCode(): number {
    return WKB_TYPE_MULTIPOLYGON;
  }

  /** @internal */
  _wktTypeName(): string {
    return 'MULTIPOLYGON';
  }

  /** @internal */
  _writeWktBody(out: string[]): void {
    this._writeMultiWktBody(out);
  }
}

/**
 * The byte reader the WKB parser reads through. Every method may answer
 * asynchronously, because the bytes of one geometry are found by parsing them
 * off the response stream.
 *
 * @internal
 */
export interface WkbByteReader {
  readUInt8(): Promise<number>;
  readInt32BE(): Promise<number>;
  readInt32LE(): Promise<number>;
  readUInt32BE(): Promise<number>;
  readUInt32LE(): Promise<number>;
  readDoubleBE(): Promise<number>;
  readDoubleLE(): Promise<number>;
}

/**
 * One read the parse routine asks of whoever drives it. Every WKB field is a
 * number, so a driver answers each read with one.
 */
type WkbRead =
  | { kind: 'uint8' }
  | { kind: 'int32'; littleEndian: boolean }
  | { kind: 'uint32'; littleEndian: boolean }
  | { kind: 'double'; littleEndian: boolean };

/**
 * The parse routine as a coroutine: it yields the reads it needs and is resumed
 * with the value of each, so one routine serves both a byte array read
 * synchronously and a response stream read asynchronously.
 */
type WkbParse<T> = Generator<WkbRead, T, number>;

const READ_UINT8: WkbRead = { kind: 'uint8' };
const READ_INT32_BE: WkbRead = { kind: 'int32', littleEndian: false };
const READ_INT32_LE: WkbRead = { kind: 'int32', littleEndian: true };
const READ_UINT32_BE: WkbRead = { kind: 'uint32', littleEndian: false };
const READ_UINT32_LE: WkbRead = { kind: 'uint32', littleEndian: true };
const READ_DOUBLE_BE: WkbRead = { kind: 'double', littleEndian: false };
const READ_DOUBLE_LE: WkbRead = { kind: 'double', littleEndian: true };

/**
 * Reads one ISO WKB geometry with the given SRID, leaving the reader on the byte
 * after it.
 *
 * ISO WKB only: EWKB, the M and ZM dimensions and unknown type codes are
 * rejected. Each geometry header carries its own byte order marker, so a nested
 * geometry may be little-endian. Lists grow as the parts are read, so a corrupt
 * count ends in an unexpected end of input rather than a large allocation.
 *
 * @internal
 */
export async function readWkb(reader: WkbByteReader, srid: number): Promise<Geometry> {
  const parse = new WkbParser(srid).readGeometry();
  let step = parse.next();
  while (!step.done) {
    step = parse.next(await readFrom(reader, step.value));
  }
  return step.value;
}

/**
 * Answers one read of the parse routine from an asynchronous reader.
 */
function readFrom(reader: WkbByteReader, read: WkbRead): Promise<number> {
  if (read.kind === 'uint8') {
    return reader.readUInt8();
  }
  if (read.kind === 'int32') {
    return read.littleEndian ? reader.readInt32LE() : reader.readInt32BE();
  }
  if (read.kind === 'uint32') {
    return read.littleEndian ? reader.readUInt32LE() : reader.readUInt32BE();
  }
  return read.littleEndian ? reader.readDoubleLE() : reader.readDoubleBE();
}

/**
 * Answers the reads of the parse routine from a byte array, and reports how many
 * bytes are left once the geometry has been read.
 */
class WkbBytes {
  #view: DataView;
  #offset = 0;

  constructor(bytes: Uint8Array) {
    this.#view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }

  /** The number of bytes not yet read. */
  get remaining(): number {
    return this.#view.byteLength - this.#offset;
  }

  read(read: WkbRead): number {
    if (read.kind === 'uint8') {
      return this.#view.getUint8(this.#take(1));
    }
    if (read.kind === 'int32') {
      return this.#view.getInt32(this.#take(4), read.littleEndian);
    }
    if (read.kind === 'uint32') {
      return this.#view.getUint32(this.#take(4), read.littleEndian);
    }
    return this.#view.getFloat64(this.#take(8), read.littleEndian);
  }

  /**
   * Advances past the next `n` bytes and returns the offset where they start.
   */
  #take(n: number): number {
    if (this.remaining < n) {
      throw new Error('Truncated WKB input: unexpected end of input');
    }
    const at = this.#offset;
    this.#offset = at + n;
    return at;
  }
}

/**
 * Formats a WKB type code the way the EWKB message reports it.
 */
function hex8(typeCode: number): string {
  return (typeCode >>> 0).toString(16).toUpperCase().padStart(8, '0');
}

/**
 * Parses ISO WKB. One parser reads one top-level geometry, so it can hold the z
 * dimension and the nesting depth of that geometry.
 */
class WkbParser {
  #srid: number;
  #depth = 0;
  #hasZ: boolean | undefined = undefined;

  constructor(srid: number) {
    this.#srid = srid;
  }

  *readGeometry(): WkbParse<Geometry> {
    const byteOrder = yield READ_UINT8;
    let littleEndian: boolean;
    if (byteOrder === WKB_BIG_ENDIAN) {
      littleEndian = false;
    } else if (byteOrder === WKB_LITTLE_ENDIAN) {
      littleEndian = true;
    } else {
      throw new Error(`Invalid WKB byte order: ${byteOrder}`);
    }

    const typeCode = yield littleEndian ? READ_INT32_LE : READ_INT32_BE;

    if ((typeCode & EWKB_Z_FLAG) !== 0 || (typeCode & EWKB_M_FLAG) !== 0 || (typeCode & EWKB_SRID_FLAG) !== 0) {
      throw new Error(
        `EWKB format detected (type code 0x${hex8(typeCode)}); ISO WKB is expected.` +
          ' EWKB with Z, M, or SRID flags is not supported.',
      );
    }
    if (typeCode >= 2001 && typeCode <= 2007) {
      throw new Error(`M dimension not supported (type code ${typeCode})`);
    }
    if (typeCode >= 3001 && typeCode <= 3007) {
      throw new Error(`ZM dimension not supported (type code ${typeCode})`);
    }

    let baseType: number;
    let z: boolean;
    if (typeCode >= 1001 && typeCode <= 1007) {
      baseType = typeCode - 1000;
      z = true;
    } else if (typeCode >= 1 && typeCode <= 7) {
      baseType = typeCode;
      z = false;
    } else {
      throw new Error(`Unknown WKB type code: ${typeCode}`);
    }

    if (this.#hasZ === undefined) {
      this.#hasZ = z;
    } else if (this.#hasZ !== z) {
      throw new Error('Mixed Z dimensions in WKB: some geometries have Z, some do not');
    }

    switch (baseType) {
      case WKB_TYPE_POINT:
        return yield* this.#readPoint(z, littleEndian);
      case WKB_TYPE_LINESTRING:
        return yield* this.#readLineString(z, littleEndian);
      case WKB_TYPE_POLYGON:
        return yield* this.#readPolygon(z, littleEndian);
      case WKB_TYPE_MULTIPOINT:
        return yield* this.#readMultiPoint(z, littleEndian);
      case WKB_TYPE_MULTILINESTRING:
        return yield* this.#readMultiLineString(z, littleEndian);
      case WKB_TYPE_MULTIPOLYGON:
        return yield* this.#readMultiPolygon(z, littleEndian);
      case WKB_TYPE_GEOMETRYCOLLECTION:
        return yield* this.#readGeometryCollection(z, littleEndian);
      default:
        throw new Error(`Unknown WKB type: ${baseType}`);
    }
  }

  /**
   * Reads a count. WKB counts are unsigned, and a count of 2^31 or more cannot
   * be addressed, so it is rejected as Java rejects the negative value it reads.
   */
  *#readCount(littleEndian: boolean, context: string): WkbParse<number> {
    const count = yield littleEndian ? READ_UINT32_LE : READ_UINT32_BE;
    if (count > INT32_MAX) {
      throw new Error(`Invalid ${context}: ${count}`);
    }
    return count;
  }

  /**
   * Reads one point. NaN x and y encode an empty point, so such a 3D point reads
   * as an empty point with a z dimension and its z ordinate is discarded.
   */
  *#readPoint(z: boolean, littleEndian: boolean): WkbParse<Point> {
    const double = littleEndian ? READ_DOUBLE_LE : READ_DOUBLE_BE;
    const x = yield double;
    const y = yield double;

    if (Number.isNaN(x) && Number.isNaN(y)) {
      if (z) {
        yield double;
      }
      return Point._emptyWithZ(this.#srid, z);
    }

    validateX(x, this.#srid);
    validateY(y, this.#srid);
    if (z) {
      const zValue = yield double;
      return Point._ofWithSrid(x, y, zValue, this.#srid);
    }
    return Point._ofWithSrid(x, y, undefined, this.#srid);
  }

  /**
   * Reads one coordinate as a vertex of a line string or a ring. A vertex is a
   * real coordinate, so NaN, which encodes an empty point, is malformed here.
   */
  *#readVertex(z: boolean, littleEndian: boolean, context: string): WkbParse<Point> {
    const double = littleEndian ? READ_DOUBLE_LE : READ_DOUBLE_BE;
    const x = yield double;
    const y = yield double;
    if (Number.isNaN(x) && Number.isNaN(y)) {
      throw new Error(`A ${context} cannot be an empty point; a vertex must be a real coordinate`);
    }
    validateX(x, this.#srid);
    validateY(y, this.#srid);
    if (z) {
      const zValue = yield double;
      return Point._ofWithSrid(x, y, zValue, this.#srid);
    }
    return Point._ofWithSrid(x, y, undefined, this.#srid);
  }

  *#readLineString(z: boolean, littleEndian: boolean): WkbParse<LineString> {
    const pointCount = yield* this.#readCount(littleEndian, 'point count');
    if (pointCount === 0) {
      return LineString._ofWithSrid([], this.#srid, z);
    }
    if (pointCount === 1) {
      throw new Error('LineString must have at least 2 points, got 1');
    }
    const points: Point[] = [];
    for (let i = 0; i < pointCount; i++) {
      points.push(yield* this.#readVertex(z, littleEndian, 'linestring vertex'));
    }
    return LineString._ofWithSrid(points, this.#srid, z);
  }

  *#readPolygon(z: boolean, littleEndian: boolean): WkbParse<Polygon> {
    const ringCount = yield* this.#readCount(littleEndian, 'ring count');
    if (ringCount === 0) {
      return Polygon._ofWithSrid(LineString._ofWithSrid([], this.#srid, z), [], this.#srid, z);
    }

    const rings: LineString[] = [];
    for (let i = 0; i < ringCount; i++) {
      const pointCount = yield* this.#readCount(littleEndian, 'point count in ring');
      if (pointCount < 4) {
        throw new Error(`Polygon ring must have at least 4 points, got ${pointCount}`);
      }
      const points: Point[] = [];
      for (let j = 0; j < pointCount; j++) {
        points.push(yield* this.#readVertex(z, littleEndian, 'ring vertex'));
      }
      const ring = LineString._ofWithSrid(points, this.#srid, z);
      validateRing(ring, i === 0 ? 'Shell ring' : 'Hole ring');
      rings.push(ring);
    }

    return Polygon._ofWithSrid(rings[0], rings.slice(1), this.#srid, z);
  }

  *#readMultiPoint(z: boolean, littleEndian: boolean): WkbParse<MultiPoint> {
    const parts = yield* this.#readParts(littleEndian, 'MultiPoint', 'Point', (part) => part instanceof Point);
    return MultiPoint._ofWithSrid(parts as Point[], this.#srid, z);
  }

  *#readMultiLineString(z: boolean, littleEndian: boolean): WkbParse<MultiLineString> {
    const parts = yield* this.#readParts(
      littleEndian,
      'MultiLineString',
      'LineString',
      (part) => part instanceof LineString,
    );
    return MultiLineString._ofWithSrid(parts as LineString[], this.#srid, z);
  }

  *#readMultiPolygon(z: boolean, littleEndian: boolean): WkbParse<MultiPolygon> {
    const parts = yield* this.#readParts(littleEndian, 'MultiPolygon', 'Polygon', (part) => part instanceof Polygon);
    return MultiPolygon._ofWithSrid(parts as Polygon[], this.#srid, z);
  }

  *#readGeometryCollection(z: boolean, littleEndian: boolean): WkbParse<GeometryCollection> {
    const parts = yield* this.#readParts(littleEndian, 'GeometryCollection', '', null);
    return GeometryCollection._ofWithSrid(parts, this.#srid, z);
  }

  /**
   * Reads the parts of a collection, rejecting a part that is not of the kind
   * the collection admits. A null `isOfKind` admits every kind.
   */
  *#readParts(
    littleEndian: boolean,
    context: string,
    kindName: string,
    isOfKind: ((part: Geometry) => boolean) | null,
  ): WkbParse<Geometry[]> {
    this.#depth++;
    validateNestingDepth(this.#depth);

    const partCount = yield* this.#readCount(littleEndian, 'geometry count');
    const parts: Geometry[] = [];
    for (let i = 0; i < partCount; i++) {
      const part = yield* this.readGeometry();
      if (isOfKind !== null && !isOfKind(part)) {
        throw new Error(`Expected ${kindName} in ${context}, got ${part.constructor.name}`);
      }
      parts.push(part);
    }

    this.#depth--;
    return parts;
  }
}
