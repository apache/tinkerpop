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

const crypto = require('crypto');
const os = require('os');
const { readFileSync } = require('fs');

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
  const buffer = crypto.randomBytes(16);
  //clear the version
  buffer[6] &= 0x0f;
  //set the version 4
  buffer[6] |= 0x40;
  //clear the variant
  buffer[8] &= 0x3f;
  //set the IETF variant
  buffer[8] |= 0x80;
  const hex = buffer.toString('hex');
  return (
    hex.substr(0, 8) +
    '-' +
    hex.substr(8, 4) +
    '-' +
    hex.substr(12, 4) +
    '-' +
    hex.substr(16, 4) +
    '-' +
    hex.substr(20, 12)
  );
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

function generateUserAgent() {
  const applicationName = (process.env.npm_package_name || 'NotAvailable').replace('_', ' ');
  let driverVersion;
  try {
    driverVersion = JSON.parse(readFileSync(__dirname + '/../package.json')).version.replace('_', ' ');
  } catch (e) {
    driverVersion = 'NotAvailable';
  }
  let runtimeVersion = osName = osVersion = cpuArch = 'NotAvailable';
  
  // process != null is equal to process != null && process != undefined
  if(process != null) {
    if(process.version != null) {
      runtimeVersion = process?.version?.replace(' ', '_');
    }
    if(process.arch != null) {
      cpuArch = process?.arch?.replace(' ', '_');
    }
  }
  
  if(os != null) {
    osName = os?.platform().replace(' ', '_');
    osVersion = os?.release().replace(' ', '_');
  }
 
  const userAgent = `${applicationName} Gremlin-Javascript.${driverVersion} ${runtimeVersion} ${osName}.${osVersion} ${cpuArch}`;

  return userAgent;
}

exports.getUserAgentHeader = function getUserAgentHeader() {
  return 'User-Agent';
};

const userAgent = generateUserAgent();

exports.getUserAgent = function getUserAgent() {
  return userAgent;
};
