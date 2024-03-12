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

import * as uuid from 'uuid';

const gremlinVersion = '4.0.0-SNAPSHOT'; // DO NOT MODIFY - Configured automatically by Maven Replacer Plugin

export function toLong(value: number | string) {
  return new Long(value);
}

export class Long {
  constructor(public value: number | string) {
    if (typeof value !== 'string' && typeof value !== 'number') {
      throw new TypeError('The value must be a string or a number');
    }
  }
}

export function getUuid() {
  // TODO: replace with `globalThis.crypto.randomUUID` once supported Node version is bump to >=19
  return uuid.v4();
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
  const os = await import('node:os');

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
