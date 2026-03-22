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

/**
 * @author Jorge Bay Gondra
 */

import DriverRemoteConnection from '../lib/driver/driver-remote-connection.js';
import Client from '../lib/driver/client.js';

let serverUrl;
let serverAuthUrl;
if (process.env.DOCKER_ENVIRONMENT === 'true') {
  serverUrl = 'http://gremlin-server-test-js:45940/gremlin';
  serverAuthUrl = 'https://gremlin-server-test-js:45941/gremlin';
} else {
  serverUrl = 'http://localhost:45940/gremlin';
  serverAuthUrl = 'https://localhost:45941/gremlin';
}

/** @returns {DriverRemoteConnection} */
export function getConnection(traversalSource) {
  return new DriverRemoteConnection(serverUrl, { traversalSource });
}

export function getDriverRemoteConnection(url, options) {
  return new DriverRemoteConnection(url, { ...options });
}

export function getClient(traversalSource) {
  return new Client(serverUrl, { traversalSource });
}

export function getAuthenticatedClient(traversalSource, interceptors) {
  return new Client(serverAuthUrl, {
    traversalSource,
    rejectUnauthorized: false,
    interceptors,
  });
}

export function getAuthenticatedConnection(traversalSource, interceptors) {
  return new DriverRemoteConnection(serverAuthUrl, {
    traversalSource,
    rejectUnauthorized: false,
    interceptors,
  });
}
