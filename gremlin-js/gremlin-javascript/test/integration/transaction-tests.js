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
import { getClient, getConnection } from '../helper.js';
import anon from '../../lib/process/anonymous-traversal.js';

let client;

describe('Transaction', function () {
  before(function () {
    client = getClient('gtx');
    return client.open();
  });
  after(function () {
    return client.close();
  });
  beforeEach(async function () {
    // Drop all vertices before each test to prevent cross-test contamination
    await client.submit("g.V().drop()");
  });

  describe('Driver-level API (client.transact())', function () {
    it('should commit transaction', async function () {
      const tx = client.transact();
      await tx.begin();
      assert.strictEqual(tx.isOpen, true);

      await tx.submit("g.addV('person').property('name','alice')");

      // Uncommitted data not visible outside the transaction
      const result1 = await client.submit("g.V().hasLabel('person').count()");
      assert.strictEqual(result1.first(), 0);

      await tx.commit();
      assert.strictEqual(tx.isOpen, false);

      // Committed data visible
      const result2 = await client.submit("g.V().hasLabel('person').count()");
      assert.strictEqual(result2.first(), 1);
    });

    it('should rollback transaction', async function () {
      const tx = client.transact();
      await tx.begin();

      await tx.submit("g.addV('person').property('name','bob')");

      await tx.rollback();
      assert.strictEqual(tx.isOpen, false);

      const result = await client.submit("g.V().hasLabel('person').count()");
      assert.strictEqual(result.first(), 0);
    });

    it('should support intra-transaction consistency', async function () {
      const tx = client.transact();
      await tx.begin();

      await tx.submit("g.addV('test').property('name','A')");

      // Read-your-own-writes
      const countResult = await tx.submit("g.V().hasLabel('test').count()");
      assert.strictEqual(countResult.first(), 1);

      await tx.submit("g.addV('test').property('name','B')");
      await tx.commit();

      const result = await client.submit("g.V().hasLabel('test').count()");
      assert.strictEqual(result.first(), 2);
    });

    it('should throw on submit after commit', async function () {
      const tx = client.transact();
      await tx.begin();
      await tx.submit("g.addV()");
      await tx.commit();

      await assert.rejects(
        () => tx.submit("g.V().count()"),
        /Transaction is not open/
      );
    });

    it('should throw on submit after rollback', async function () {
      const tx = client.transact();
      await tx.begin();
      await tx.submit("g.addV()");
      await tx.rollback();

      await assert.rejects(
        () => tx.submit("g.V().count()"),
        /Transaction is not open/
      );
    });

    it('should throw on double begin', async function () {
      const tx = client.transact();
      await tx.begin();

      await assert.rejects(
        () => tx.begin(),
        /Transaction already started/
      );
    });

    it('should throw on commit when not open', async function () {
      const tx = client.transact();

      await assert.rejects(
        () => tx.commit(),
        /Transaction is not open/
      );
    });

    it('should throw on rollback when not open', async function () {
      const tx = client.transact();

      await assert.rejects(
        () => tx.rollback(),
        /Transaction is not open/
      );
    });

    it('should return undefined transactionId before begin', async function () {
      const tx = client.transact();
      assert.strictEqual(tx.transactionId, undefined);

      await tx.begin();
      assert.ok(tx.transactionId);
      assert.ok(tx.transactionId.length > 0);
    });

    it('should rollback on close by default', async function () {
      const tx = client.transact();
      await tx.begin();
      await tx.submit("g.addV('person').property('name','close_test')");

      await tx.close();
      assert.strictEqual(tx.isOpen, false);

      const result = await client.submit("g.V().hasLabel('person').count()");
      assert.strictEqual(result.first(), 0);
    });

    it('should isolate concurrent transactions', async function () {
      const tx1 = client.transact();
      await tx1.begin();
      const tx2 = client.transact();
      await tx2.begin();

      await tx1.submit("g.addV('tx1')");
      await tx2.submit("g.addV('tx2')");

      // tx1 should not see tx2's data
      const r1 = await tx1.submit("g.V().hasLabel('tx2').count()");
      assert.strictEqual(r1.first(), 0);
      const r2 = await tx2.submit("g.V().hasLabel('tx1').count()");
      assert.strictEqual(r2.first(), 0);

      await tx1.commit();
      await tx2.commit();

      const result1 = await client.submit("g.V().hasLabel('tx1').count()");
      assert.strictEqual(result1.first(), 1);
      const result2 = await client.submit("g.V().hasLabel('tx2').count()");
      assert.strictEqual(result2.first(), 1);
    });

    it('should open and close many transactions sequentially', async function () {
      const numTransactions = 50;
      for (let i = 0; i < numTransactions; i++) {
        const tx = client.transact();
        await tx.begin();
        await tx.submit("g.addV('stress')");
        await tx.commit();
      }

      const result = await client.submit("g.V().hasLabel('stress').count()");
      assert.strictEqual(result.first(), numTransactions);
    });

    it('should keep transaction open after traversal error', async function () {
      const tx = client.transact();
      await tx.begin();
      await tx.submit("g.addV('good_vertex')");

      try {
        await tx.submit("g.V().fail()");
      } catch (e) {
        // expected
      }

      assert.strictEqual(tx.isOpen, true);
      await tx.rollback();

      const result = await client.submit("g.V().hasLabel('good_vertex').count()");
      assert.strictEqual(result.first(), 0);
    });

    it('should not allow begin after commit', async function () {
      const tx = client.transact();
      await tx.begin();
      await tx.commit();

      await assert.rejects(
        () => tx.begin(),
        /Transaction already started/
      );
    });

    it('should rollback on client close', async function () {
      const txClient = getClient('gtx');
      const tx = txClient.transact();
      await tx.begin();
      await tx.submit("g.addV('client_close_test')");

      await txClient.close();

      assert.strictEqual(tx.isOpen, false);

      const result = await client.submit("g.V().hasLabel('client_close_test').count()");
      assert.strictEqual(result.first(), 0);
    });

    it('should not allow begin after rollback', async function () {
      const tx = client.transact();
      await tx.begin();
      await tx.rollback();

      await assert.rejects(
        () => tx.begin(),
        /Transaction already started/
      );
    });

    it('should throw on double commit', async function () {
      const tx = client.transact();
      await tx.begin();
      await tx.commit();

      await assert.rejects(
        () => tx.commit(),
        /Transaction is not open/
      );
    });

    it('should throw on double rollback', async function () {
      const tx = client.transact();
      await tx.begin();
      await tx.rollback();

      await assert.rejects(
        () => tx.rollback(),
        /Transaction is not open/
      );
    });

    it('should multi rollback transactions', async function () {
      const tx1 = client.transact();
      await tx1.begin();
      const tx2 = client.transact();
      await tx2.begin();

      await tx1.submit("g.addV('multi_rb1')");
      await tx2.submit("g.addV('multi_rb2')");

      await tx1.rollback();
      const r1 = await client.submit("g.V().hasLabel('multi_rb1').count()");
      assert.strictEqual(r1.first(), 0);

      await tx2.rollback();
      const r2 = await client.submit("g.V().hasLabel('multi_rb2').count()");
      assert.strictEqual(r2.first(), 0);
    });

    it('should multi commit and rollback', async function () {
      const tx1 = client.transact();
      await tx1.begin();
      const tx2 = client.transact();
      await tx2.begin();

      await tx1.submit("g.addV('multi_cr1')");
      await tx2.submit("g.addV('multi_cr2')");

      await tx1.commit();
      const r1 = await client.submit("g.V().hasLabel('multi_cr1').count()");
      assert.strictEqual(r1.first(), 1);

      await tx2.rollback();
      const r2 = await client.submit("g.V().hasLabel('multi_cr2').count()");
      assert.strictEqual(r2.first(), 0);
    });

    it('should isolate transactional and non-transactional requests', async function () {
      const tx = client.transact();
      await tx.begin();
      await tx.submit("g.addV('tx_data')");

      // Non-transactional read should not see uncommitted data
      const result1 = await client.submit("g.V().hasLabel('tx_data').count()");
      assert.strictEqual(result1.first(), 0);

      await tx.commit();

      const result2 = await client.submit("g.V().hasLabel('tx_data').count()");
      assert.strictEqual(result2.first(), 1);
    });
  });

  describe('Traversal API (g.tx())', function () {
    it('should work with driver remote connection', async function () {
      const connection = getConnection('gtx');
      const g = anon.traversal().withRemote(connection);

      const tx = g.tx();
      const gtx = await tx.begin();
      await gtx.addV('val').iterate();
      await tx.commit();

      const count = await g.V().hasLabel('val').count().next();
      assert.strictEqual(count.value, 1);

      await connection.close();
    });

    it('should return same transaction from gtx.tx()', async function () {
      const connection = getConnection('gtx');
      const g = anon.traversal().withRemote(connection);

      const tx = g.tx();
      const gtx = await tx.begin();
      const sameTx = gtx.tx();
      assert.strictEqual(sameTx, tx);

      await tx.rollback();
      await connection.close();
    });

    it('should throw on begin from gtx.tx()', async function () {
      const connection = getConnection('gtx');
      const g = anon.traversal().withRemote(connection);

      const tx = g.tx();
      const gtx = await tx.begin();
      const sameTx = gtx.tx();

      await assert.rejects(
        () => sameTx.begin(),
        /Transaction already started/
      );

      await tx.rollback();
      await connection.close();
    });

    it('should commit via gtx.tx().commit()', async function () {
      const connection = getConnection('gtx');
      const g = anon.traversal().withRemote(connection);

      const tx = g.tx();
      const gtx = await tx.begin();
      await gtx.addV('gtx_commit_test').iterate();
      await gtx.tx().commit();

      const count = await g.V().hasLabel('gtx_commit_test').count().next();
      assert.strictEqual(count.value, 1);

      await connection.close();
    });

    it('should reject begin on non-transactional graph', async function () {
      const nonTxClient = getClient('gclassic');

      const tx = nonTxClient.transact();
      try {
        await tx.begin();
        assert.fail('Expected error on begin for non-transactional graph');
      } catch (e) {
        assert.ok(e.statusMessage.includes('does not support transactions'));
      }

      await nonTxClient.close();
    });

    it('should clean up on begin failure', async function () {
      const nonTxClient = getClient('gclassic');

      const tx = nonTxClient.transact();
      try {
        await tx.begin();
      } catch (e) {
        // expected
      }

      assert.strictEqual(tx.isOpen, false);
      assert.strictEqual(tx.transactionId, undefined);

      // Cannot begin again
      await assert.rejects(
        () => tx.begin(),
        /Transaction already started/
      );

      await nonTxClient.close();
    });
  });
});
