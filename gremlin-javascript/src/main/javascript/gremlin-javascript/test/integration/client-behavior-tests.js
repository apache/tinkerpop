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

'use strict';

const assert = require('assert');
const helper = require('../helper');
const {getUserAgent} = require("../../lib/utils");

let client;
let settings;

describe('Client', function () {
    before(function () {
        client = helper.getGremlinSocketServerClient('gmodern');
        settings = helper.getGremlinSocketServerSettings();
        return client.open();
    });
    after(function () {
        return client.close();
    });
    describe('#submit()', function () {
        it('should reconnect after server closes connection', async function () {
            let connectionClosed = false;
            await client.submit('1', null, {requestId: settings.CLOSE_CONNECTION_REQUEST_ID})
                .catch(function(error){
                    assert.equal(error.toString(), 'Error: Connection has been closed.');
                    connectionClosed = true;
                });

            assert.equal(connectionClosed, true);

            let result = await client.submit('1', null, {requestId: settings.SINGLE_VERTEX_REQUEST_ID})
            assert.ok(result);
        });
        it('should include user agent in handshake request', async function () {
            let result = await client.submit('1', null, {requestId: settings.USER_AGENT_REQUEST_ID});

            assert.strictEqual(result.first(), await getUserAgent());
        });
        it('should not include user agent in handshake request if disabled', async function () {
            let noUserAgentClient = helper.getGremlinSocketServerClientNoUserAgent('gmodern');
            let result = await noUserAgentClient.submit('1', null,
                {requestId: settings.USER_AGENT_REQUEST_ID});

            assert.strictEqual(result.first(), "");

            await noUserAgentClient.close();
        });
        it('should not request permessage deflate compression by default', async function () {
            const result = await client.submit('1', null, {requestId: settings.SEC_WEBSOCKET_EXTENSIONS});
            const returnedExtensions = result.first()
            assert.ok(returnedExtensions == undefined || !returnedExtensions.includes("permessage-deflate"))
        });
        it('should not request permessage deflate compression when disabled', async function () {
            const noCompressionClient = helper.getGremlinSocketServerClientWithOptions('gmodern',
                {enableCompression: false});
            const result = await noCompressionClient.submit('1', null,
                {requestId: settings.SEC_WEBSOCKET_EXTENSIONS});

            const returnedExtensions = result.first()
            assert.ok(returnedExtensions == undefined || !returnedExtensions.includes("permessage-deflate"))

            await noCompressionClient.close();
        });
        it('should request permessage deflate compression when enabled', async function () {
            const compressionClient = helper.getGremlinSocketServerClientWithOptions('gmodern',
                {enableCompression: true});
            const result = await compressionClient.submit('1', null,
                {requestId: settings.SEC_WEBSOCKET_EXTENSIONS});

            const returnedExtensions = result.first()
            assert.ok(returnedExtensions.includes("permessage-deflate"))

            await compressionClient.close();
        });
        it('should send per request settings to server', async function () {
            const resultSet = await client.submit('1', null, {
                requestId: settings.PER_REQUEST_SETTINGS_REQUEST_ID,
                evaluationTimeout: 1234,
                batchSize: 12,
                userAgent: 'helloWorld',
                materializeProperties: 'tokens'
            })
            const expectedResult = `requestId=${settings.PER_REQUEST_SETTINGS_REQUEST_ID} evaluationTimeout=1234, batchSize=12, userAgent=helloWorld, materializeProperties=tokens`;
            assert.equal(expectedResult, resultSet.first());
        });
    });
});
