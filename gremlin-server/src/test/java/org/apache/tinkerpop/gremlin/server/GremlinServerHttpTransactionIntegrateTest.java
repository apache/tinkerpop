/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.http.Consts;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import org.apache.tinkerpop.gremlin.server.handler.TransactionManager;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.util.ser.SerTokens.TOKEN_DATA;
import static org.apache.tinkerpop.gremlin.util.ser.SerTokens.TOKEN_RESULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Server-side integration tests for HTTP transaction protocol.
 * 
 * These tests bypass the driver entirely and use a raw Apache HTTP client to hit the server's HTTP endpoint directly.
 * This validates that the server returns the correct status codes and error messages independent of any client-side
 * guards.
 */
public class GremlinServerHttpTransactionIntegrateTest extends AbstractGremlinServerIntegrationTest {
    private final String GTX = "gtx";
    private final ObjectMapper mapper = new ObjectMapper();
    private CloseableHttpClient client;

    @Before
    public void createHttpClient() {
        client = HttpClients.createDefault();
    }

    @After
    public void closeHttpClient() throws Exception {
        client.close();
    }

    @Override
    public Settings overrideSettings(final Settings settings) {
        settings.channelizer = HttpChannelizer.class.getName();
        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldRejectRequestWhenMaxConcurrentTransactionsExceeded":
                settings.maxConcurrentTransactions = 1;
                break;
            case "shouldTimeoutFreeSlotUnderMaxConcurrentTransactions":
                settings.maxConcurrentTransactions = 1;
                settings.transactionTimeout = 1000;
                break;
            case "shouldTimeoutIdleTransactionWithNoOperations":
                settings.transactionTimeout = 1;
                break;
            case "shouldTimeoutAndRejectLateCommit":
            case "shouldTrackTransactionCountAccurately":
                settings.transactionTimeout = 1000;
                break;
            case "shouldRollbackAbandonedTransaction":
                settings.transactionTimeout = 300;
                break;
        }
        return settings;
    }

    /**
     * Sends a JSON POST request and returns the response. Caller must close the response.
     */
    private CloseableHttpResponse postJson(final CloseableHttpClient client, final String json) throws Exception {
        final HttpPost post = new HttpPost(TestClientFactory.createURLString());
        post.addHeader("Content-Type", "application/json");
        post.setEntity(new StringEntity(json, Consts.UTF_8));
        return client.execute(post);
    }

    /**
     * Sends a begin transaction request for the given graph alias and returns the server-generated transaction ID.
     */
    private String beginTx(final CloseableHttpClient client, final String graphAlias) throws Exception {
        try (final CloseableHttpResponse response = postJson(client,
                "{\"gremlin\":\"g.tx().begin()\",\"g\":\"" + graphAlias + "\"}")) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            final String txIdHeader = response.getFirstHeader(Tokens.Headers.TRANSACTION_ID).getValue();
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            final String txIdBody = node.get(TOKEN_RESULT).
                    get(TOKEN_DATA).
                    get(GraphSONTokens.VALUEPROP).get(0).
                    get(GraphSONTokens.VALUEPROP).get(1).
                    asText();
            assertNotNull(txIdHeader);
            assertEquals(txIdHeader, txIdBody);
            return txIdHeader;
        }
    }

    /**
     * Sends a traversal within an existing transaction.
     */
    private CloseableHttpResponse submitInTx(final CloseableHttpClient client,
                                             final String txId,
                                             final String gremlin,
                                             final String graphAlias)
            throws Exception {
        return postJson(client,
                "{\"gremlin\":\"" + gremlin + "\",\"g\":\"" + graphAlias + "\",\"transactionId\":\"" + txId + "\"}");
    }

    /**
     * Sends a commit for an existing transaction.
     */
    private CloseableHttpResponse commitTx(final CloseableHttpClient client,
                                           final String txId, final String graphAlias) throws Exception {
        return postJson(client,
                "{\"gremlin\":\"g.tx().commit()\",\"g\":\"" + graphAlias + "\",\"transactionId\":\"" + txId + "\"}");
    }

    /**
     * Sends a rollback for an existing transaction.
     */
    private CloseableHttpResponse rollbackTx(final CloseableHttpClient client,
                                             final String txId, final String graphAlias) throws Exception {
        return postJson(client,
                "{\"gremlin\":\"g.tx().rollback()\",\"g\":\"" + graphAlias + "\",\"transactionId\":\"" + txId + "\"}");
    }

    /**
     * Sends a non-transactional traversal (no transactionId).
     */
    private CloseableHttpResponse submitNonTx(final CloseableHttpClient client,
                                              final String gremlin, final String graphAlias) throws Exception {
        return postJson(client,
                "{\"gremlin\":\"" + gremlin + "\",\"g\":\"" + graphAlias + "\"}");
    }

    /**
     * Extracts the integer count from a typical count() response.
     */
    private int extractCount(final CloseableHttpResponse response) throws Exception {
        final String json = EntityUtils.toString(response.getEntity());
        final JsonNode node = mapper.readTree(json);
        return node.get("result").get(TOKEN_DATA)
                .get(GraphSONTokens.VALUEPROP).get(0)
                .get(GraphSONTokens.VALUEPROP).intValue();
    }

    /**
     * Extracts the status message from the response body's status field.
     */
    private String extractStatusMessage(final CloseableHttpResponse response) throws Exception {
        final String json = EntityUtils.toString(response.getEntity());
        final JsonNode node = mapper.readTree(json);
        return node.get("status").get("message").asText();
    }

    @Test
    public void shouldNotBeginTransactionWithUserProvidedId() throws Exception {
        final String txId = beginTx(client, GTX);

        try (final CloseableHttpResponse r = postJson(client,
                "{\"gremlin\":\"g.tx().begin()\",\"g\":\"" + GTX + "\",\"transactionId\":\"" + txId + "\"}")) {
            // Depending on whether the transaction is still open on the server when the second request arrives, there
            // may be two different errors that the server throws.
            assertTrue(r.getStatusLine().getStatusCode() == 404 || r.getStatusLine().getStatusCode() == 400);
            final String msg = extractStatusMessage(r);
            assertTrue(msg.contains("Begin transaction request cannot have a user-supplied transactionId") ||
                    msg.contains("Transaction not found"));
        }
    }

    @Test
    public void shouldReturn404ForInvalidCommit() throws Exception {
        // Can't commit on non-existent transaction.
        try (final CloseableHttpResponse r = commitTx(client, "fakeId", GTX)) {
            assertEquals(404, r.getStatusLine().getStatusCode());
            final String msg = extractStatusMessage(r);
            assertTrue(msg.contains("Transaction not found"));
        }

        final String txId = beginTx(client, GTX);

        try (final CloseableHttpResponse r = submitInTx(client, txId, "g.addV('test')", GTX)) {
            assertEquals(200, r.getStatusLine().getStatusCode());
        }
        try (final CloseableHttpResponse r = commitTx(client, txId, GTX)) {
            assertEquals(200, r.getStatusLine().getStatusCode());
        }

        // #10: submit traversal on committed tx -> 404
        try (final CloseableHttpResponse r = submitInTx(client, txId, "g.V().count()", GTX)) {
            assertEquals(404, r.getStatusLine().getStatusCode());
            final String msg = extractStatusMessage(r);
            assertTrue(msg.contains("Transaction not found"));
        }

        // #13: commit again on committed tx -> 404
        try (final CloseableHttpResponse r = commitTx(client, txId, GTX)) {
            assertEquals(404, r.getStatusLine().getStatusCode());
            final String msg = extractStatusMessage(r);
            assertTrue(msg.contains("Transaction not found"));
        }
    }

    @Test
    public void shouldReturn404ForInvalidRollback() throws Exception {
        // Can't rollback a non-existent transaction.
        try (final CloseableHttpResponse r = rollbackTx(client, "fakeId", GTX)) {
            assertEquals(404, r.getStatusLine().getStatusCode());
            final String msg = extractStatusMessage(r);
            assertTrue(msg.contains("Transaction not found"));
        }

        final String txId = beginTx(client, GTX);

        // add a vertex and rollback
        try (final CloseableHttpResponse r = submitInTx(client, txId, "g.addV('test')", GTX)) {
            assertEquals(200, r.getStatusLine().getStatusCode());
        }
        try (final CloseableHttpResponse r = rollbackTx(client, txId, GTX)) {
            assertEquals(200, r.getStatusLine().getStatusCode());
        }

        // #11: submit traversal on rolled-back tx -> 404
        try (final CloseableHttpResponse r = submitInTx(client, txId, "g.V().count()", GTX)) {
            final String msg = extractStatusMessage(r);
            assertEquals(404, r.getStatusLine().getStatusCode());
            assertTrue(msg.contains("Transaction not found"));
        }

        // #14: rollback again on rolled-back tx -> 404
        try (final CloseableHttpResponse r = rollbackTx(client, txId, GTX)) {
            assertEquals(404, r.getStatusLine().getStatusCode());
            final String msg = extractStatusMessage(r);
            assertTrue(msg.contains("Transaction not found"));
        }
    }

    @Test
    public void shouldReturnValidTransactionId() throws Exception {
        // #20: begin returns a valid transaction ID
        final String txId1 = beginTx(client, GTX);
        assertNotNull(txId1);
        assertFalse(txId1.isBlank());

        // #27: second begin returns a different ID
        final String txId2 = beginTx(client, GTX);
        assertNotNull(txId2);
        assertFalse(txId2.isBlank());
        assertNotEquals(txId1, txId2);
    }

    @Test
    public void shouldReturn404ForInvalidTransactionId() throws Exception {
        final String fakeTxId = UUID.randomUUID().toString();

        try (final CloseableHttpResponse r = submitInTx(client, fakeTxId, "g.V().count()", GTX)) {
            assertEquals(404, r.getStatusLine().getStatusCode());
            final String msg = extractStatusMessage(r);
            assertTrue(msg.contains("Transaction not found"));
        }
    }

    @Test
    public void shouldRejectRequestWhenMaxConcurrentTransactionsExceeded() throws Exception {
        // open one transaction (fills the limit)
        beginTx(client, GTX);

        // try to open another -- should fail with 503 (SERVICE_UNAVAILABLE)
        try (final CloseableHttpResponse r = postJson(client,
                "{\"gremlin\":\"g.tx().begin()\",\"g\":\"" + GTX + "\"}")) {
            assertEquals(503, r.getStatusLine().getStatusCode());
            final String msg = extractStatusMessage(r);
            assertTrue(msg.contains("Maximum concurrent transactions exceeded"));
        }
    }

    @Test
    public void shouldRejectEmptyTransactionId() throws Exception {
        try (final CloseableHttpResponse r = submitInTx(client, "invalid", "g.tx().begin()", GTX)) {
            assertEquals(400, r.getStatusLine().getStatusCode());
            final String msg = extractStatusMessage(r);
            assertTrue(msg.contains("Begin transaction request cannot have a user-supplied transactionId"));
        }
    }

    @Test
    public void shouldTimeoutIdleTransactionWithNoOperations() throws Exception {
        final String txId = beginTx(client, GTX);

        // wait for the transaction to timeout (configured at 1ms)
        Thread.sleep(1000);

        // the transaction should be gone
        try (final CloseableHttpResponse r = submitInTx(client, txId, "g.V().count()", GTX)) {
            assertEquals(404, r.getStatusLine().getStatusCode());
            final String msg = extractStatusMessage(r);
            assertTrue(msg.contains("Transaction not found"));
        }
    }

    @Test
    public void shouldTimeoutAndRejectLateCommit() throws Exception {
        final String txId = beginTx(client, GTX);

        // add a vertex
        try (final CloseableHttpResponse r = submitInTx(client, txId, "g.addV('timeout_test')", GTX)) {
            assertEquals(200, r.getStatusLine().getStatusCode());
        }

        // wait for timeout (configured at 1000ms)
        Thread.sleep(2000);

        // attempt commit -- should fail with 404
        try (final CloseableHttpResponse r = commitTx(client, txId, GTX)) {
            assertEquals(404, r.getStatusLine().getStatusCode());
            final String msg = extractStatusMessage(r);
            assertTrue(msg.contains("Transaction not found"));
        }

        // verify data was not persisted
        try (final CloseableHttpResponse r = submitNonTx(client, "g.V().hasLabel('timeout_test').count()", GTX)) {
            assertEquals(200, r.getStatusLine().getStatusCode());
            assertEquals(0, extractCount(r));
        }
    }

    @Test
    public void shouldTrackTransactionCountAccurately() throws Exception {
        final TransactionManager txManager = server.getServerGremlinExecutor().getTransactionManager();
        assertEquals(0, txManager.getActiveTransactionCount());

        // open 3 transactions
        final String txId1 = beginTx(client, GTX);
        final String txId2 = beginTx(client, GTX);
        final String txId3 = beginTx(client, GTX);

        assertEquals(3, txManager.getActiveTransactionCount());

        // commit one
        commitTx(client, txId1, GTX);
        assertEquals(2, txManager.getActiveTransactionCount());

        // rollback one
        rollbackTx(client, txId2, GTX);
        assertEquals(1, txManager.getActiveTransactionCount());

        // let the third one timeout
        Thread.sleep(1500);
        assertEquals(0, txManager.getActiveTransactionCount());
    }

    @Test
    public void shouldTimeoutFreeSlotUnderMaxConcurrentTransactions() throws Exception {
        final TransactionManager tm = server.getServerGremlinExecutor().getTransactionManager();

        // fill the single slot
        beginTx(client, GTX);
        assertEquals(1, tm.getActiveTransactionCount());

        // wait for timeout to reclaim the slot
        Thread.sleep(2000);
        assertEquals(0, tm.getActiveTransactionCount());

        // now a new transaction should succeed
        final String txId = beginTx(client, GTX);
        assertNotNull(txId);
        assertFalse(txId.isBlank());
    }

    @Test
    public void shouldReturn404ForAllOperationsOnClosedTransaction() throws Exception {
        final String txId1 = beginTx(client, GTX);
        try (final CloseableHttpResponse r = submitInTx(client, txId1, "g.addV('test38')", GTX)) {
            assertEquals(200, r.getStatusLine().getStatusCode());
        }
        try (final CloseableHttpResponse r = commitTx(client, txId1, GTX)) {
            assertEquals(200, r.getStatusLine().getStatusCode());
        }

        // rollback-after-commit
        try (final CloseableHttpResponse r = rollbackTx(client, txId1, GTX)) {
            assertEquals(404, r.getStatusLine().getStatusCode());
        }
        // traversal-after-commit
        try (final CloseableHttpResponse r = submitInTx(client, txId1, "g.V().count()", GTX)) {
            assertEquals(404, r.getStatusLine().getStatusCode());
        }

        final String txId2 = beginTx(client, GTX);
        try (final CloseableHttpResponse r = submitInTx(client, txId2, "g.addV('test38b')", GTX)) {
            assertEquals(200, r.getStatusLine().getStatusCode());
        }
        try (final CloseableHttpResponse r = rollbackTx(client, txId2, GTX)) {
            assertEquals(200, r.getStatusLine().getStatusCode());
        }

        // commit-after-rollback
        try (final CloseableHttpResponse r = commitTx(client, txId2, GTX)) {
            assertEquals(404, r.getStatusLine().getStatusCode());
        }
        // traversal-after-rollback
        try (final CloseableHttpResponse r = submitInTx(client, txId2, "g.V().count()", GTX)) {
            assertEquals(404, r.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void shouldNotLeakDataWhenTraversalQueuedBehindCommit() throws Exception {
        final String txId = beginTx(client, GTX);

        // add vertices and an edge in the transaction
        try (final CloseableHttpResponse r = submitInTx(client, txId, "g.addV().property(T.id, 1)", GTX)) {
            assertEquals(200, r.getStatusLine().getStatusCode());
        }
        try (final CloseableHttpResponse r = submitInTx(client, txId, "g.addV().property(T.id, 2)", GTX)) {
            assertEquals(200, r.getStatusLine().getStatusCode());
        }

        // Fire three requests concurrently: a long traversal to occupy the server executor, then a commit that queues
        // behind it, then a short query that queues behind the commit. The short query should fail with 404 because the
        // commit closes the transaction first.
        final ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            final Future<CloseableHttpResponse> longFuture = executor.submit(() ->
                    submitInTx(client, txId, "g.V().repeat(both()).times(1000)", GTX));
            Thread.sleep(50);

            final Future<CloseableHttpResponse> commitFuture = executor.submit(() ->
                    commitTx(client, txId, GTX));
            Thread.sleep(50);

            final Future<CloseableHttpResponse> shortFuture = executor.submit(() ->
                    submitInTx(client, txId, "g.V().count()", GTX));

            // collect responses
            try (final CloseableHttpResponse ignored = longFuture.get(30, TimeUnit.SECONDS)) {
                // it doesn't matter what the long traversal returns, only that it ran
            }
            try (final CloseableHttpResponse commitResp = commitFuture.get(30, TimeUnit.SECONDS)) {
                assertEquals(200, commitResp.getStatusLine().getStatusCode());
            }
            try (final CloseableHttpResponse shortResp = shortFuture.get(30, TimeUnit.SECONDS)) {
                assertEquals(404, shortResp.getStatusLine().getStatusCode());
                final String msg = extractStatusMessage(shortResp);
                assertTrue(msg.contains("Transaction not found"));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void shouldRollbackAbandonedTransaction() throws Exception {
        final String txId1 = beginTx(client, GTX);
        submitInTx(client, txId1, "g.addV()", GTX);

        // wait for server-side timeout
        Thread.sleep(1000);

        // reconnect and verify data was not persisted
        try (final CloseableHttpResponse r = submitNonTx(client, "g.V().count()", GTX)) {
            assertEquals(0, extractCount(r));
        }
    }

    @Test
    public void shouldRejectMismatchedGraphAliasInTransaction() throws Exception {
        final String txId = beginTx(client, GTX);

        // send a request with the same txId but a different graph alias
        try (final CloseableHttpResponse r = submitInTx(client, txId, "g.V().count()", "gclassic")) {
            final int status = r.getStatusLine().getStatusCode();
            assertTrue("Expected error status for alias mismatch, got " + status,
                    status == 400 || status == 404 || status == 500);
        }
    }

    @Test
    public void shouldRequireGraphAliasOnBeginRequest() throws Exception {
        // begin with no g alias -- should default to "g" which doesn't support transactions
        try (final CloseableHttpResponse response = postJson(client,
                "{\"gremlin\":\"g.tx().begin()\"}")) {
            final int status = response.getStatusLine().getStatusCode();
            assertEquals(400, status);
            assertTrue(extractStatusMessage(response).contains("Graph does not support transactions"));
        }

        // begin with an invalid alias -- should fail
        try (final CloseableHttpResponse response = postJson(client,
                "{\"gremlin\":\"g.tx().begin()\",\"g\":\"nonexistent\"}")) {
            final int status = response.getStatusLine().getStatusCode();
            assertEquals(400, status);
            assertTrue(extractStatusMessage(response).contains("Could not alias"));
        }

        // begin with valid alias -- should succeed (positive case)
        final String txId = beginTx(client, GTX);
        assertNotNull(txId);
        assertFalse(txId.isBlank());
    }

    @Test
    public void shouldRequireTransactionIdOnCommitAndRollback() throws Exception {
        // commit with no transactionId -- this is just "g.tx().commit()" with no txId,
        // which the server treats as a begin (since there's no txId). But the gremlin is
        // "g.tx().commit()" not "g.tx().begin()", so the server should route to commit
        // handling which requires a txId. The exact behavior depends on the routing logic.
        try (final CloseableHttpResponse response = postJson(client,
                "{\"gremlin\":\"g.tx().commit()\",\"g\":\"gtx\"}")) {
            // without a transactionId, this should not succeed as a commit
            final int status = response.getStatusLine().getStatusCode();
            // the server may treat this as a non-transactional request or reject it
            // either way it should not be a successful commit of a transaction
            assertEquals(400, status);
            assertTrue(extractStatusMessage(response).contains("only allowed in transactional requests"));
        }

        // rollback with no transactionId -- same logic
        try (final CloseableHttpResponse response = postJson(client,
                "{\"gremlin\":\"g.tx().rollback()\",\"g\":\"gtx\"}")) {
            final int status = response.getStatusLine().getStatusCode();
            assertEquals(400, status);
            assertTrue(extractStatusMessage(response).contains("only allowed in transactional requests"));
        }
    }

    @Test
    public void shouldRejectBeginOnNonTransactionalGraph() throws Exception {
        // gclassic is backed by TinkerGraph (non-transactional)
        try (final CloseableHttpResponse response = postJson(client,
                "{\"gremlin\":\"g.tx().begin()\",\"g\":\"gclassic\"}")) {
            final int status = response.getStatusLine().getStatusCode();
            assertTrue("Expected error for non-transactional graph, got " + status,
                    status == 400 || status == 500);
        }
    }

    @Test
    public void shouldRoundTripTransactionIdWithGraphBinary() throws Exception {
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();

        // begin via GraphBinary
        final ByteBuf beginReq = serializer.serializeRequestAsBinary(
                RequestMessage.build("g.tx().begin()").addG(GTX).create(),
                new UnpooledByteBufAllocator(false));
        final HttpPost beginPost = new HttpPost(TestClientFactory.createURLString());
        beginPost.addHeader(HttpHeaders.CONTENT_TYPE, Serializers.GRAPHBINARY_V4.getValue());
        beginPost.addHeader(HttpHeaders.ACCEPT, Serializers.GRAPHBINARY_V4.getValue());
        beginPost.setEntity(new ByteArrayEntity(beginReq.array()));

        String txId;
        try (final CloseableHttpResponse response = client.execute(beginPost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            final ResponseMessage rm = serializer.readChunk(toByteBuf(response.getEntity()), true);
            final List<?> data = rm.getResult().getData();
            assertNotNull(data);
            assertTrue(data.size() > 0);
            // the data should contain a map with transactionId
            final Map<?, ?> map = (java.util.Map<?, ?>) data.get(0);
            txId = (String) map.get(Tokens.ARGS_TRANSACTION_ID);
            assertNotNull(txId);
            assertFalse(txId.isBlank());
        }

        // addV via GraphBinary
        final ByteBuf addVReq = serializer.serializeRequestAsBinary(
                RequestMessage.build("g.addV('binary_test')").addG(GTX).addTransactionId(txId).create(),
                new UnpooledByteBufAllocator(false));
        final HttpPost addVPost = new HttpPost(TestClientFactory.createURLString());
        addVPost.addHeader(HttpHeaders.CONTENT_TYPE, Serializers.GRAPHBINARY_V4.getValue());
        addVPost.addHeader(HttpHeaders.ACCEPT, Serializers.GRAPHBINARY_V4.getValue());
        addVPost.setEntity(new ByteArrayEntity(addVReq.array()));
        try (final CloseableHttpResponse response = client.execute(addVPost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
        }

        // commit via GraphBinary
        final ByteBuf commitReq = serializer.serializeRequestAsBinary(
                RequestMessage.build("g.tx().commit()").addG(GTX).addTransactionId(txId).create(),
                new UnpooledByteBufAllocator(false));
        final HttpPost commitPost = new HttpPost(TestClientFactory.createURLString());
        commitPost.addHeader(HttpHeaders.CONTENT_TYPE, Serializers.GRAPHBINARY_V4.getValue());
        commitPost.addHeader(HttpHeaders.ACCEPT, Serializers.GRAPHBINARY_V4.getValue());
        commitPost.setEntity(new ByteArrayEntity(commitReq.array()));
        try (final CloseableHttpResponse response = client.execute(commitPost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
        }

        // verify data persisted
        try (final CloseableHttpResponse r = submitNonTx(client, "g.V().hasLabel('binary_test').count()", GTX)) {
            assertEquals(200, r.getStatusLine().getStatusCode());
            assertEquals(1, extractCount(r));
        }
    }

    private static ByteBuf toByteBuf(final org.apache.http.HttpEntity httpEntity) throws java.io.IOException {
        final byte[] asArray = EntityUtils.toByteArray(httpEntity);
        return Unpooled.wrappedBuffer(asArray);
    }
    @Test
    public void shouldRoundTripTransactionIdWithGraphSON() throws Exception {
        final GraphSONMessageSerializerV4 serializer = new GraphSONMessageSerializerV4();

        // begin via GraphSON
        final ByteBuf beginReq = serializer.serializeRequestAsBinary(
                RequestMessage.build("g.tx().begin()").addG(GTX).create(),
                new UnpooledByteBufAllocator(false));
        final HttpPost beginPost = new HttpPost(TestClientFactory.createURLString());
        beginPost.addHeader(HttpHeaders.CONTENT_TYPE, Serializers.GRAPHSON_V4.getValue());
        beginPost.addHeader(HttpHeaders.ACCEPT, Serializers.GRAPHSON_V4.getValue());
        beginPost.setEntity(new ByteArrayEntity(beginReq.array()));

        String txId;
        try (final CloseableHttpResponse response = client.execute(beginPost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(Serializers.GRAPHSON_V4.getValue(), response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            txId = node.get("result").get(TOKEN_DATA)
                    .get(GraphSONTokens.VALUEPROP).get(0)
                    .get(GraphSONTokens.VALUEPROP).get(1)
                    .asText();

            assertNotNull(txId);
            assertFalse(txId.isBlank());
        }

        // addV via GraphSON
        final ByteBuf addVReq = serializer.serializeRequestAsBinary(
                RequestMessage.build("g.addV('graphson_test')").addG(GTX).addTransactionId(txId).create(),
                new UnpooledByteBufAllocator(false));
        final HttpPost addVPost = new HttpPost(TestClientFactory.createURLString());
        addVPost.addHeader(HttpHeaders.CONTENT_TYPE, Serializers.GRAPHSON_V4.getValue());
        addVPost.addHeader(HttpHeaders.ACCEPT, Serializers.GRAPHSON_V4.getValue());
        addVPost.setEntity(new ByteArrayEntity(addVReq.array()));
        try (final CloseableHttpResponse response = client.execute(addVPost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
        }

        // commit via GraphSON
        final ByteBuf commitReq = serializer.serializeRequestAsBinary(
                RequestMessage.build("g.tx().commit()").addG(GTX).addTransactionId(txId).create(),
                new UnpooledByteBufAllocator(false));
        final HttpPost commitPost = new HttpPost(TestClientFactory.createURLString());
        commitPost.addHeader(HttpHeaders.CONTENT_TYPE, Serializers.GRAPHSON_V4.getValue());
        commitPost.addHeader(HttpHeaders.ACCEPT, Serializers.GRAPHSON_V4.getValue());
        commitPost.setEntity(new ByteArrayEntity(commitReq.array()));
        try (final CloseableHttpResponse response = client.execute(commitPost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
        }

        // verify data persisted
        try (final CloseableHttpResponse r = submitNonTx(client, "g.V().hasLabel('graphson_test').count()", GTX)) {
            assertEquals(200, r.getStatusLine().getStatusCode());
            assertEquals(1, extractCount(r));
        }
    }

    @Test
    public void shouldReturnTransactionIdHeader() throws Exception {
        final String beginJson = "{\"gremlin\":\"g.tx().begin()\",\"g\":\"" + GTX + "\"}";
        String txIdFromBegin;
        try (final CloseableHttpResponse response = postJson(client, beginJson)) {
            assertEquals(200, response.getStatusLine().getStatusCode());

            // header must be present
            assertNotNull(response.getFirstHeader(Tokens.Headers.TRANSACTION_ID));
            final String txIdFromHeader = response.getFirstHeader(Tokens.Headers.TRANSACTION_ID).getValue();
            assertNotNull(txIdFromHeader);
            assertFalse(txIdFromHeader.isBlank());

            // body must contain the same transaction ID
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            txIdFromBegin = node.get(TOKEN_RESULT).get(TOKEN_DATA)
                    .get(GraphSONTokens.VALUEPROP).get(0)
                    .get(GraphSONTokens.VALUEPROP).get(1)
                    .asText();
            assertNotNull(txIdFromBegin);
            assertFalse(txIdFromBegin.isBlank());

            assertEquals("Transaction ID in header and body must match on begin",
                    txIdFromHeader, txIdFromBegin);
        }

        try (final CloseableHttpResponse response = submitInTx(client, txIdFromBegin, "g.addV('dual_test')", GTX)) {
            assertEquals(200, response.getStatusLine().getStatusCode());

            // Body doesn't contain the Transaction ID for Traversals in transactions.
            assertEquals(txIdFromBegin, response.getFirstHeader(Tokens.Headers.TRANSACTION_ID).getValue());
        }

        try (final CloseableHttpResponse response = commitTx(client, txIdFromBegin, GTX)) {
            assertEquals(200, response.getStatusLine().getStatusCode());

            final JsonNode jsonResponse = mapper.readTree(EntityUtils.toString(response.getEntity()));
            final String txIdFromCommit = jsonResponse.get(TOKEN_RESULT).get(TOKEN_DATA)
                    .get(GraphSONTokens.VALUEPROP).get(0)
                    .get(GraphSONTokens.VALUEPROP).get(1)
                    .asText();
            assertEquals(txIdFromBegin, txIdFromCommit);

            assertEquals("Transaction ID in header must match on submit",
                    txIdFromBegin, response.getFirstHeader(Tokens.Headers.TRANSACTION_ID).getValue());
        }
    }
}
