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
 * A module containing any utility functions.
 * @author Jorge Bay Gondra
 */

const gremlinVersion = '4.0.0-SNAPSHOT'; // DO NOT MODIFY - Configured automatically by Maven Replacer Plugin

const INT64_MIN = -9223372036854775808n;
const INT64_MAX = 9223372036854775807n;

export const INT32_MIN = -2147483648;
export const INT32_MAX = 2147483647;

export function toLong(value: number | string | bigint) {
  return new Long(value);
}

export class Long {
  readonly type = 'long';

  constructor(public readonly value: number | string | bigint) {
    if (typeof value !== 'string' && typeof value !== 'number' && typeof value !== 'bigint') {
      throw new TypeError('The value must be a string, a number, or a bigint');
    }
    if (typeof value === 'string') {
      if (!/^(?:0|-?[1-9]\d*)$/.test(value)) {
        throw new TypeError('Long value must be a valid integer');
      }
      const n = BigInt(value);
      if (n < INT64_MIN || n > INT64_MAX) {
        throw new RangeError('Long value is outside int64 range');
      }
    }
    if (typeof value === 'number') {
      if (!Number.isInteger(value)) {
        throw new TypeError('Long value must be an integer');
      }
      if (value > Number.MAX_SAFE_INTEGER || value < Number.MIN_SAFE_INTEGER) {
        throw new RangeError('Long number values outside safe integer range lose precision; use string or bigint');
      }
    }
    if (typeof value === 'bigint') {
      if (value < INT64_MIN || value > INT64_MAX) {
        throw new RangeError('Long value is outside int64 range');
      }
    }
  }

  valueOf(): number {
    if (typeof this.value === 'number') return this.value;
    const big = typeof this.value === 'bigint' ? this.value : BigInt(this.value);
    if (big > BigInt(Number.MAX_SAFE_INTEGER) || big < BigInt(Number.MIN_SAFE_INTEGER)) {
      throw new RangeError('Long value is outside safe integer range');
    }
    return Number(big);
  }

  [Symbol.toPrimitive](hint: string) {
    if (hint === 'string') return String(this.value);
    return this.valueOf();
  }

  toJSON() {
    if (typeof this.value === 'bigint') return this.value.toString();
    return this.value;
  }
}

export class Int {
  readonly type = 'int';
  constructor(public readonly value: number) {
    if (typeof value !== 'number') {
      throw new TypeError('Int value must be a number');
    }
    if (!Number.isFinite(value) || !Number.isInteger(value)) {
      throw new TypeError('Int value must be a finite integer');
    }
    if (value < INT32_MIN || value > INT32_MAX) {
      throw new RangeError('Int value must be within int32 range');
    }
  }
  valueOf() { return this.value; }
  [Symbol.toPrimitive](hint: string) { return hint === 'string' ? String(this.value) : this.value; }
  toJSON() { return this.value; }
}

export class Float {
  readonly type = 'float';
  constructor(public readonly value: number) {
    if (typeof value !== 'number') {
      throw new TypeError('Float value must be a number');
    }
  }
  valueOf() { return this.value; }
  [Symbol.toPrimitive](hint: string) { return hint === 'string' ? String(this.value) : this.value; }
  toJSON() { return this.value; }
}

export class Double {
  readonly type = 'double';
  constructor(public readonly value: number) {
    if (typeof value !== 'number') {
      throw new TypeError('Double value must be a number');
    }
  }
  valueOf() { return this.value; }
  [Symbol.toPrimitive](hint: string) { return hint === 'string' ? String(this.value) : this.value; }
  toJSON() { return this.value; }
}

export class Short {
  readonly type = 'short';
  constructor(public readonly value: number) {
    if (typeof value !== 'number') {
      throw new TypeError('Short value must be a number');
    }
    if (!Number.isFinite(value) || !Number.isInteger(value)) {
      throw new TypeError('Short value must be a finite integer');
    }
    if (value < -32768 || value > 32767) {
      throw new RangeError('Short value must be within int16 range');
    }
  }
  valueOf() { return this.value; }
  [Symbol.toPrimitive](hint: string) { return hint === 'string' ? String(this.value) : this.value; }
  toJSON() { return this.value; }
}

export class Byte {
  readonly type = 'byte';
  constructor(public readonly value: number) {
    if (typeof value !== 'number') {
      throw new TypeError('Byte value must be a number');
    }
    if (!Number.isFinite(value) || !Number.isInteger(value)) {
      throw new TypeError('Byte value must be a finite integer');
    }
    if (value < -128 || value > 127) {
      throw new RangeError('Byte value must be within int8 range');
    }
  }
  valueOf() { return this.value; }
  [Symbol.toPrimitive](hint: string) { return hint === 'string' ? String(this.value) : this.value; }
  toJSON() { return this.value; }
}

/**
 * Recursive deep-equality check, in the spirit of Node's `util.isDeepStrictEqual` but implemented
 * without importing `node:util` so it also works in the browser. Handles the value shapes that
 * flow through this GLV: primitives (via `Object.is`, so `NaN`/`-0` compare correctly), Date,
 * RegExp, typed arrays/Buffer, Map, Set, Array, and plain objects/class instances (own-enumerable
 * keys, same prototype). Not a general-purpose substitute for `assert.deepStrictEqual`.
 */
export function deepEqual(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) return true;
  if (typeof a !== 'object' || typeof b !== 'object' || a === null || b === null) return false;
  if (Object.getPrototypeOf(a) !== Object.getPrototypeOf(b)) return false;

  if (a instanceof Date) return a.getTime() === (b as Date).getTime();
  if (a instanceof RegExp) return a.toString() === (b as RegExp).toString();

  if (ArrayBuffer.isView(a) && ArrayBuffer.isView(b)) {
    const viewA = a as unknown as Uint8Array;
    const viewB = b as unknown as Uint8Array;
    if (viewA.length !== viewB.length) return false;
    for (let i = 0; i < viewA.length; i++) {
      if (viewA[i] !== viewB[i]) return false;
    }
    return true;
  }

  if (a instanceof Map) {
    const mapB = b as Map<unknown, unknown>;
    if (a.size !== mapB.size) return false;
    for (const [key, value] of a) {
      if (!mapB.has(key) || !deepEqual(value, mapB.get(key))) return false;
    }
    return true;
  }

  if (a instanceof Set) {
    const setB = b as Set<unknown>;
    if (a.size !== setB.size) return false;
    for (const value of a) {
      if (![...setB].some((other) => deepEqual(value, other))) return false;
    }
    return true;
  }

  if (Array.isArray(a)) {
    const arrayB = b as unknown[];
    if (a.length !== arrayB.length) return false;
    return a.every((value, index) => deepEqual(value, arrayB[index]));
  }

  const keysA = Object.keys(a);
  const keysB = Object.keys(b);
  if (keysA.length !== keysB.length) return false;
  return keysA.every((key) => Object.hasOwn(b, key) && deepEqual((a as any)[key], (b as any)[key]));
}

export function toInt(value: number) { return new Int(value); }
export function toFloat(value: number) { return new Float(value); }
export function toDouble(value: number) { return new Double(value); }
export function toShort(value: number) { return new Short(value); }
export function toByte(value: number) { return new Byte(value); }

export function unwrap(value: Float): number;
export function unwrap(value: Double): number;
export function unwrap(value: Int): number;
export function unwrap(value: Long): number | string | bigint;
export function unwrap(value: Short): number;
export function unwrap(value: Byte): number;
export function unwrap<T>(value: T): T;
export function unwrap(value: any): any {
  if (value instanceof Long || value instanceof Int || value instanceof Float ||
      value instanceof Double || value instanceof Short || value instanceof Byte) {
    return value.value;
  }
  return value;
}

export function getUuid() {
  return globalThis.crypto.randomUUID();
}

export const emptyArray = Object.freeze([]) as any as any[];

export class ImmutableMap<K, V> extends Map<K, V> implements ReadonlyMap<K, V> {
  constructor(iterable?: Iterable<[K, V]>) {
    super(iterable);
  }

  set() {
    return this;
  }

  ['delete']() {
    return false;
  }

  clear() {}
}

async function generateNodeUserAgent() {
  // The specifier is built in a separate statement (not inlined) so bundlers (esbuild/webpack/Vite)
  // see a non-literal expression and leave this as a runtime dynamic import instead of trying to
  // statically resolve/bundle the Node built-in "node:os" for a browser target - esbuild constant-
  // folds an inline `import('node:' + 'os')` back into a literal and fails the same as `import('node:os')`.
  // This branch is never reached in the browser: getUserAgent() below returns via the `navigator`
  // check before calling this function.
  const osModuleSpecifier = 'node:' + 'os';
  const os = await import(osModuleSpecifier);

  const applicationName = (process?.env.npm_package_name ?? 'NotAvailable').replace('_', ' ');
  let runtimeVersion;
  let osName;
  let osVersion;
  let cpuArch;
  runtimeVersion = osName = osVersion = cpuArch = 'NotAvailable';
  if (process != null) {
    if (process.version) {
      runtimeVersion = process.version.replace(' ', '_');
    }
    if (process.arch) {
      cpuArch = process.arch.replace(' ', '_');
    }
  }

  if (os != null) {
    osName = os.platform().replace(' ', '_');
    osVersion = os.release().replace(' ', '_');
  }

  const userAgent = `${applicationName} Gremlin-Javascript.${gremlinVersion} ${runtimeVersion} ${osName}.${osVersion} ${cpuArch}`;

  return userAgent;
}

export function getUserAgentHeader() {
  return 'User-Agent';
}

export const getUserAgent = async () => {
  if ('navigator' in globalThis) {
    return globalThis.navigator.userAgent;
  }

  if (process?.versions?.node !== undefined) {
    return await generateNodeUserAgent();
  }

  return undefined;
};

export const toArrayBuffer = (buffer: Buffer) =>
  buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);

export const DeferredPromise = <T>() => {
  let resolve = (value: T) => {};
  let reject = (reason: unknown) => {};

  const promise = new Promise<T>((_resolve, _reject) => {
    resolve = _resolve;
    reject = _reject;
  });

  return Object.assign(promise, { resolve, reject });
};
