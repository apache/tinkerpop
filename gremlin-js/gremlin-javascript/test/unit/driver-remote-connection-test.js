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
import DriverRemoteConnection from '../../lib/driver/driver-remote-connection.js';
import GremlinLang from '../../lib/process/gremlin-lang.js';
import { OptionsStrategy } from '../../lib/process/traversal-strategy.js';

describe('DriverRemoteConnection', function () {
  function createConnection(capture, options = {}) {
    const connection = new DriverRemoteConnection('http://localhost:8182/gremlin',
      { traversalSource: 'mySource', connectOnStartup: false, ...options });
    connection._client = {
      stream(gremlin, parameters, requestOptions) {
        capture({ gremlin, parameters, requestOptions });
        return (async function* () {})();
      },
    };
    return connection;
  }

  it('does not send traversal source as a parameter', async function () {
    let captured;
    const connection = createConnection(args => captured = args);
    const gremlinLang = new GremlinLang().addStep('V');

    await connection.submit(gremlinLang);

    assert.ok(!captured.requestOptions.parameters || !captured.requestOptions.parameters.includes("'g':"));
  });

  it('sends explicit GremlinLang parameters without adding traversal source', async function () {
    let captured;
    const connection = createConnection(args => captured = args);
    const gremlinLang = new GremlinLang().addStep('V');
    gremlinLang.getParameters().set('x', 42);

    await connection.submit(gremlinLang);

    assert.strictEqual(captured.requestOptions.parameters, "['x':42]");
  });

  describe('language', function () {
    function withLanguage(language) {
      return new GremlinLang()
        .addStep('V')
        .addSource('withStrategies', [new OptionsStrategy({ language })]);
    }

    it('does not set a language when none is configured', async function () {
      let captured;
      const connection = createConnection(args => captured = args);

      await connection.submit(new GremlinLang().addStep('V'));

      assert.strictEqual(captured.requestOptions.language, undefined);
    });

    it('sends the connection-level language', async function () {
      let captured;
      const connection = createConnection(args => captured = args, { language: 'my-engine' });

      await connection.submit(new GremlinLang().addStep('V'));

      assert.strictEqual(captured.requestOptions.language, 'my-engine');
    });

    it('sends the language set with with_()', async function () {
      let captured;
      const connection = createConnection(args => captured = args);

      await connection.submit(withLanguage('my-engine'));

      assert.strictEqual(captured.requestOptions.language, 'my-engine');
    });

    it('lets with_() override the connection-level language', async function () {
      let captured;
      const connection = createConnection(args => captured = args, { language: 'connection-engine' });

      await connection.submit(withLanguage('source-engine'));

      assert.strictEqual(captured.requestOptions.language, 'source-engine');
    });

    it('sends the connection-level language on a transaction-bound connection', async function () {
      let captured;
      // Transaction.gtx() builds its connection from the client options plus the transaction id
      const connection = createConnection(args => captured = args, { language: 'my-engine' });
      const txConnection = new DriverRemoteConnection(connection.url, connection.options, 'tx-1');
      txConnection._client = connection._client;

      await txConnection.submit(new GremlinLang().addStep('V'));

      assert.strictEqual(captured.requestOptions.language, 'my-engine');
      assert.strictEqual(captured.requestOptions.transactionId, 'tx-1');
    });

    it('does not render the language into the gremlin text', async function () {
      let captured;
      const connection = createConnection(args => captured = args);

      await connection.submit(withLanguage('my-engine'));

      assert.ok(!captured.gremlin.includes('language'));
    });
  });
});
