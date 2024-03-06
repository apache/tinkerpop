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
'use strict';

const uuid = require('uuid');

const gremlinVersion = '4.0.0-SNAPSHOT'; // DO NOT MODIFY - Configured automatically by Maven Replacer Plugin

exports.toLong = function toLong(value) {
  return new Long(value);
};

const Long = (exports.Long = function Long(value) {
  if (typeof value !== 'string' && typeof value !== 'number') {
    throw new TypeError('The value must be a string or a number');
  }
  this.value = value.toString();
});

exports.getUuid = function getUuid() {
  // TODO: replace with `globalThis.crypto.randomUUID` once supported Node version is bump to >=19
  return uuid.v4();
};

exports.emptyArray = Object.freeze([]);

class ImmutableMap extends Map {
  constructor(iterable) {
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

exports.ImmutableMap = ImmutableMap;

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

exports.getUserAgentHeader = function getUserAgentHeader() {
  return 'User-Agent';
};

exports.getUserAgent = async () => {
  if ('navigator' in globalThis) {
    return globalThis.navigator.userAgent;
  }

  if (process?.versions?.node !== undefined) {
    return await generateNodeUserAgent();
  }

  return undefined;
};

/**
 * @param {Buffer} buffer
 * @returns {ArrayBuffer}
 */
const toArrayBuffer = (buffer) => buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);

exports.toArrayBuffer = toArrayBuffer;

const DeferredPromise = () => {
  let resolve = (value) => {};
  let reject = (reason) => {};

  const promise = new Promise((_resolve, _reject) => {
    resolve = _resolve;
    reject = _reject;
  });

  return Object.assign(promise, { resolve, reject });
};

module.exports.DeferredPromise = DeferredPromise;
