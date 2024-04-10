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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.Channelizer;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.junit.Test;

import java.awt.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.StringEndsWith.endsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HttpDriverIntegrateTest extends AbstractGremlinServerIntegrationTest {

    @Override
    public Settings overrideSettings(final Settings settings) {
        settings.channelizer = HttpChannelizer.class.getName();
        return settings;
    }

//    @Test
//    public void shouldSubmitScriptWithGraphSON() throws Exception {
//        final Cluster cluster = TestClientFactory.build().create();
//        try {
//            final Client client = cluster.connect();
//            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
//        } catch (Exception ex) {
//            throw ex;
//        } finally {
//            cluster.close();
//        }
//    }

    @Test
    public void shouldSubmitScriptWithGraphBinary() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect();
            // default chunk size is 64
            assertEquals(100, client.submit("new int[100]").all().get().size());
        } catch (Exception ex) {
            throw ex;
        } finally {
            cluster.close();
        }
    }

//    @Test
//    public void shouldSubmitBytecodeWithGraphSON() throws Exception {
//        final Cluster cluster = TestClientFactory.build()
//                .channelizer(Channelizer.HttpChannelizer.class)
//                .serializer(Serializers.GRAPHSON_V4)
//                .create();
//        try {
//            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
//            final String result = g.inject("2").toList().get(0);
//            assertEquals("2", result);
//        } catch (Exception ex) {
//            throw ex;
//        } finally {
//            cluster.close();
//        }
//    }

//    @Test
//    public void shouldGetErrorForBytecodeWithUntypedGraphSON() throws Exception {
//        final Cluster cluster = TestClientFactory.build().create();
//        try {
//            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
//            g.inject("2").toList();
//            fail("Exception expected");
//        } catch (EncoderException ex) {
//            assertThat(ex.getMessage(), allOf(containsString("An error occurred during serialization of this request"),
//                    containsString("it could not be sent to the server - Reason: only GraphSON3 and GraphBinary recommended for serialization of Bytecode requests, but used org.apache.tinkerpop.gremlin.")));
//        } finally {
//            cluster.close();
//        }
//    }

    @Test
    public void shouldSubmitBytecodeWithGraphBinary() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
            final String result = g.inject("2").toList().get(0);
            assertEquals("2", result);
        } catch (Exception ex) {
            throw ex;
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailToUseSession() {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect("shouldFailToUseSession");
            client.submit("1+1").all().get();
            fail("Can't use session with HTTP");
        } catch (Exception ex) {
            final Throwable t = ExceptionUtils.getRootCause(ex);
            assertEquals("Cannot use sessions or tx() with HttpChannelizer", t.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailToUseTx() {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
            final Transaction tx = g.tx();
            final GraphTraversalSource gtx = tx.begin();
            gtx.inject("1").toList();
            fail("Can't use tx() with HTTP");
        } catch (Exception ex) {
            final Throwable t = ExceptionUtils.getRootCause(ex);
            assertEquals("Cannot use sessions or tx() with HttpChannelizer", t.getMessage());
        } finally {
            cluster.close();
        }
    }

    // !!!
    @Test
    public void shouldDeserializeErrorWithGraphBinary() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "doesNotExist"));
            g.V().next();
            fail("Expected exception to be thrown.");
        } catch (Exception ex) {
            assert ex.getMessage().contains("The traversal source [doesNotExist] for alias [g] is not configured on the server.");
        } finally {
            cluster.close();
        }
    }

//    @Test
//    public void shouldDeserializeErrorWithGraphSON() throws Exception {
//        final Cluster cluster = TestClientFactory.build().create();
//        try {
//            final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "doesNotExist"));
//            g.V().next();
//            fail("Expected exception to be thrown.");
//        } catch (Exception ex) {
//            assert ex.getMessage().contains("Could not rebind");
//        } finally {
//            cluster.close();
//        }
//    }

    @Test
    public void shouldReportErrorWhenRequestCantBeSerialized() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect();

            try {
                final Map<String, Object> params = new HashMap<>();
                params.put("r", Color.RED);
                client.submit("r", params).all().get();
                fail("Should have thrown exception over bad serialization");
            } catch (Exception ex) {
                final Throwable inner = ExceptionHelper.getRootCause(ex);
                assertThat(inner, instanceOf(ResponseException.class));
                assertEquals(ResponseStatusCode.REQUEST_ERROR_SERIALIZATION, ((ResponseException) inner).getResponseStatusCode());
                assertTrue(ex.getMessage().contains("An error occurred during serialization of this request"));
            }

            // should not die completely just because we had a bad serialization error.  that kind of stuff happens
            // from time to time, especially in the console if you're just exploring.
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    // not supported on server side now
    @Test
    public void shouldProcessTraversalInterruption() {
        final Cluster cluster = TestClientFactory.build().create();

        try {
            final Client client = cluster.connect();
            client.submit("g.inject(1).sideEffect{Thread.sleep(5000)}").all().get();
            fail("Should have timed out");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(ResponseStatusCode.SERVER_ERROR_TIMEOUT, re.getResponseStatusCode());
        } finally {
            cluster.close();
        }
    }

    // not supported on server side now
    @Test
    public void shouldProcessEvalInterruption() {
        final Cluster cluster = TestClientFactory.build().create();

        try {
            final Client client = cluster.connect();
            client.submit("Thread.sleep(5000);'done'").all().get();
            fail("Should have timed out");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(ResponseStatusCode.SERVER_ERROR_TIMEOUT, re.getResponseStatusCode());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailWithBadClientSideSerialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {

            final ResultSet results = client.submit("java.awt.Color.RED");

            try {
                results.all().join();
                fail("Should have thrown exception over bad serialization");
            } catch (Exception ex) {
                final Throwable inner = ExceptionHelper.getRootCause(ex);
                assertThat(inner, instanceOf(ResponseException.class));
                assertEquals(ResponseStatusCode.SERVER_ERROR_SERIALIZATION, ((ResponseException) inner).getResponseStatusCode());
            }

            // should not die completely just because we had a bad serialization error.  that kind of stuff happens
            // from time to time, especially in the console if you're just exploring.
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailWithScriptExecutionException() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            try {
                final ResultSet results = client.submit("1/0");
                results.all().join();
                fail("Should have thrown exception over division by zero");
            } catch (Exception ex) {
                final Throwable inner = ExceptionHelper.getRootCause(ex);
                assertTrue(inner instanceof ResponseException);
                assertThat(inner.getMessage(), endsWith("Division by zero"));

                final ResponseException rex = (ResponseException) inner;
                // todo: not implemented
//                assertEquals("java.lang.ArithmeticException", rex.getRemoteExceptionHierarchy().get().get(0));
//                assertEquals(1, rex.getRemoteExceptionHierarchy().get().size());
//                assertThat(rex.getRemoteStackTrace().get(), containsString("Division by zero"));
            }

            // should not die completely just because we had a bad serialization error.  that kind of stuff happens
            // from time to time, especially in the console if you're just exploring.
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldProcessRequestsOutOfOrder() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect();

            final ResultSet rsFive = client.submit("Thread.sleep(5000);'five'");
            final ResultSet rsZero = client.submit("'zero'");

            final CompletableFuture<List<Result>> futureFive = rsFive.all();
            final CompletableFuture<List<Result>> futureZero = rsZero.all();

            assertFalse(futureFive.isDone());
            assertEquals("zero", futureZero.get().get(0).getString());

            assertFalse(futureFive.isDone());
            assertEquals("five", futureFive.get(10, TimeUnit.SECONDS).get(0).getString());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldWaitForAllResultsToArrive() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect();

            final AtomicInteger checked = new AtomicInteger(0);
            final ResultSet results = client.submit("[1,2,3,4,5,6,7,8,9]");
            while (!results.allItemsAvailable()) {
                assertTrue(results.getAvailableItemCount() < 10);
                checked.incrementAndGet();
                Thread.sleep(100);
            }

            assertTrue(checked.get() > 0);
            assertEquals(9, results.getAvailableItemCount());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldStream() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            final ResultSet results = client.submit("[1,2,3,4,5,6,7,8,9]");
            final AtomicInteger counter = new AtomicInteger(0);
            results.stream().map(i -> i.get(Integer.class) * 2).forEach(i -> assertEquals(counter.incrementAndGet() * 2, Integer.parseInt(i.toString())));
            assertEquals(9, counter.get());
            assertThat(results.allItemsAvailable(), is(true));

            // cant stream it again
            assertThat(results.stream().iterator().hasNext(), is(false));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldIterate() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            final ResultSet results = client.submit("[1,2,3,4,5,6,7,8,9]");
            final Iterator<Result> itty = results.iterator();
            final AtomicInteger counter = new AtomicInteger(0);
            while (itty.hasNext()) {
                counter.incrementAndGet();
                assertEquals(counter.get(), itty.next().getInt());
            }

            assertEquals(9, counter.get());
            assertThat(results.allItemsAvailable(), is(true));

            // can't stream it again
            assertThat(results.iterator().hasNext(), is(false));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldGetSomeThenSomeMore() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            final ResultSet results = client.submit("[1,2,3,4,5,6,7,8,9]");
            final CompletableFuture<List<Result>> batch1 = results.some(5);
            final CompletableFuture<List<Result>> batch2 = results.some(5);
            final CompletableFuture<List<Result>> batchNothingLeft = results.some(5);

            assertEquals(5, batch1.get().size());
            assertEquals(1, batch1.get().get(0).getInt());
            assertEquals(2, batch1.get().get(1).getInt());
            assertEquals(3, batch1.get().get(2).getInt());
            assertEquals(4, batch1.get().get(3).getInt());
            assertEquals(5, batch1.get().get(4).getInt());

            assertEquals(4, batch2.get().size());
            assertEquals(6, batch2.get().get(0).getInt());
            assertEquals(7, batch2.get().get(1).getInt());
            assertEquals(8, batch2.get().get(2).getInt());
            assertEquals(9, batch2.get().get(3).getInt());

            assertEquals(0, batchNothingLeft.get().size());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldGetOneThenSomeThenSomeMore() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            final ResultSet results = client.submit("[1,2,3,4,5,6,7,8,9]");
            final Result one = results.one();
            final CompletableFuture<List<Result>> batch1 = results.some(4);
            final CompletableFuture<List<Result>> batch2 = results.some(5);
            final CompletableFuture<List<Result>> batchNothingLeft = results.some(5);

            assertEquals(1, one.getInt());

            assertEquals(4, batch1.get().size());
            assertEquals(2, batch1.get().get(0).getInt());
            assertEquals(3, batch1.get().get(1).getInt());
            assertEquals(4, batch1.get().get(2).getInt());
            assertEquals(5, batch1.get().get(3).getInt());

            assertEquals(4, batch2.get().size());
            assertEquals(6, batch2.get().get(0).getInt());
            assertEquals(7, batch2.get().get(1).getInt());
            assertEquals(8, batch2.get().get(2).getInt());
            assertEquals(9, batch2.get().get(3).getInt());

            assertEquals(0, batchNothingLeft.get().size());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldCloseWithServerDown() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            cluster.connect().init();

            stopServer();
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailWithBadServerSideSerialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();
        try {

            final ResultSet results = client.submit("TinkerGraph.open().variables()");

            try {
                results.all().join();
                fail();
            } catch (Exception ex) {
                final Throwable inner = ExceptionHelper.getRootCause(ex);
                assertTrue(inner instanceof ResponseException);
                assertEquals(ResponseStatusCode.SERVER_ERROR_SERIALIZATION, ((ResponseException) inner).getResponseStatusCode());
            }

            // should not die completely just because we had a bad serialization error.  that kind of stuff happens
            // from time to time, especially in the console if you're just exploring.
            assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldNotThrowNoSuchElementException() {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            // this should return "nothing" - there should be no exception
            assertNull(client.submit("g.V().has('name','kadfjaldjfla')").one());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEvalInGremlinLang() {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            final RequestOptions ro = RequestOptions.build().language("gremlin-lang").create();
            assertEquals(111, client.submit("g.inject(111)", ro).one().getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEvalInGremlinLangWithParameters() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            final RequestOptions ro = RequestOptions.build().language("gremlin-lang").
                    addParameter("x", 100).
                    addParameter("y", "test").
                    addParameter("z", true).create();
            final List<Result> l = client.submit("g.inject(x, y, z)", ro).all().get();
            assertEquals(100, l.get(0).getInt());
            assertEquals("test", l.get(1).getString());
            assertTrue(l.get(2).getBoolean());
        } finally {
            cluster.close();
        }
    }
}
