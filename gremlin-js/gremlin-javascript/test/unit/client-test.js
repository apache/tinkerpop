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

import assert from 'assert';
import Client from '../../lib/driver/client.js';

describe('Client', function () {
  const query = 'customQuery';

  it('should submit request with default traversalSource', function () {
    const connectionMock = {
      submit: function (requestMessage) {
        assert.strictEqual(requestMessage.getGremlin(), query);
        assert.strictEqual(requestMessage.getG(), 'g');
        assert.strictEqual(requestMessage.getLanguage(), 'gremlin-lang');
        return Promise.resolve();
      }
    };

    const customClient = new Client('http://localhost:9321', {traversalSource: 'g', connectOnStartup: false});
    customClient._connection = connectionMock;
    customClient.submit(query)
  });

  it('should submit request with custom traversalSource', function () {
    const connectionMock = {
      submit: function (requestMessage) {
        assert.strictEqual(requestMessage.getGremlin(), query);
        assert.strictEqual(requestMessage.getG(), 'gCustom');
        return Promise.resolve();
      }
    };

    const customClient = new Client('http://localhost:9321', {traversalSource: 'gCustom', connectOnStartup: false});
    customClient._connection = connectionMock;
    customClient.submit(query)
  });

  it('should fill batchSize from the connection default when unset', function () {
    const connectionMock = {
      defaultBatchSize: 64,
      submit: function (requestMessage) {
        assert.strictEqual(requestMessage.getBatchSize(), 64);
        return Promise.resolve();
      }
    };

    const customClient = new Client('http://localhost:9321', {connectOnStartup: false});
    customClient._connection = connectionMock;
    customClient.submit(query);
  });

  it('should let a per-request batchSize override the connection default', function () {
    const connectionMock = {
      defaultBatchSize: 64,
      submit: function (requestMessage) {
        assert.strictEqual(requestMessage.getBatchSize(), 10);
        return Promise.resolve();
      }
    };

    const customClient = new Client('http://localhost:9321', {connectOnStartup: false});
    customClient._connection = connectionMock;
    customClient.submit(query, null, { batchSize: 10 });
  });

  it('should forward an explicit bulkResults:false', function () {
    const connectionMock = {
      submit: function (requestMessage) {
        assert.strictEqual(requestMessage.getBulkResults(), false);
        return Promise.resolve();
      }
    };

    const customClient = new Client('http://localhost:9321', {connectOnStartup: false});
    customClient._connection = connectionMock;
    customClient.submit(query, null, { bulkResults: false });
  });

  it('should forward an explicit bulkResults:true', function () {
    const connectionMock = {
      submit: function (requestMessage) {
        assert.strictEqual(requestMessage.getBulkResults(), true);
        return Promise.resolve();
      }
    };

    const customClient = new Client('http://localhost:9321', {connectOnStartup: false});
    customClient._connection = connectionMock;
    customClient.submit(query, null, { bulkResults: true });
  });

  it('should fill bulkResults from the connection-level option when unset per-request', function () {
    const connectionMock = {
      bulkResults: true,
      submit: function (requestMessage) {
        assert.strictEqual(requestMessage.getBulkResults(), true);
        return Promise.resolve();
      }
    };

    const customClient = new Client('http://localhost:9321', {connectOnStartup: false});
    customClient._connection = connectionMock;
    customClient.submit(query);
  });

  it('should let a per-request bulkResults:false override the connection-level true', function () {
    const connectionMock = {
      bulkResults: true,
      submit: function (requestMessage) {
        assert.strictEqual(requestMessage.getBulkResults(), false);
        return Promise.resolve();
      }
    };

    const customClient = new Client('http://localhost:9321', {connectOnStartup: false});
    customClient._connection = connectionMock;
    customClient.submit(query, null, { bulkResults: false });
  });

  it('should not set bulkResults when neither per-request nor connection-level is set', function () {
    const connectionMock = {
      bulkResults: undefined,
      submit: function (requestMessage) {
        assert.strictEqual(requestMessage.getBulkResults(), undefined);
        return Promise.resolve();
      }
    };

    const customClient = new Client('http://localhost:9321', {connectOnStartup: false});
    customClient._connection = connectionMock;
    customClient.submit(query);
  });
});
