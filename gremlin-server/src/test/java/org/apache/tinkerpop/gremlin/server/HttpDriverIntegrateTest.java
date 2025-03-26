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

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.junit.Ignore;
import org.junit.Test;

import java.awt.Color;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.list;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
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

    @Test
    public void shouldSubmitScriptWithGraphBinary() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect();
            // default chunk size is 64
            assertEquals(100, client.submit("g.inject(0).repeat(inject(0)).times(99)").all().get().size());
        } catch (Exception ex) {
            throw ex;
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailWhenNeeded() {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();
        try {
            final RequestOptions ro = RequestOptions.build().language("gremlin-lang").create();
            client.submit("g.inject(1).fail('Good bye, world!')", ro).all().get();
            fail("should throw exception");
        } catch (Exception ex) {
            final Throwable inner = ExceptionHelper.getRootCause(ex);
            assertThat(inner, instanceOf(ResponseException.class));
            assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, ((ResponseException) inner).getResponseStatusCode());
            assertTrue(ex.getMessage().contains("Good bye, world!"));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldHandleObjectBiggerThen8kb() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect();
            final RequestOptions ro = RequestOptions.build().language("gremlin-groovy").create();
            final List r = client.submit("[\" \".repeat(200000), \" \".repeat(100000)]", ro).all().get();
            assertEquals(200000, ((Result) r.get(0)).getString().length());
            assertEquals(100000, ((Result) r.get(1)).getString().length());
        } catch (Exception ex) {
            throw ex;
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldSubmitTraversalWithGraphBinary() {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final GraphTraversalSource g = traversal().with(DriverRemoteConnection.using(cluster));
            final String result = g.inject("2").toList().get(0);
            assertEquals("2", result);
        } catch (Exception ex) {
            throw ex;
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldSubmitGremlinWithCollectionAsArgument() {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final GraphTraversalSource g = traversal().with(DriverRemoteConnection.using(cluster));
            final List<?> result = g.inject(Arrays.asList("test", 2L, null)).toList().get(0);
            assertThat(result, is(Arrays.asList("test", 2L, null)));
        } catch (Exception ex) {
            throw ex;
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldSubmitGremlinWithMergeV() {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final GraphTraversalSource g = traversal().with(DriverRemoteConnection.using(cluster));
            g.addV("person").property("name", "marko")
                    .property(list, "age", 29)
                    .property(list, "age", 31)
                    .property(list, "age", 32)
                    .iterate();

            final long result = g.mergeV(CollectionUtil.asMap("name", "marko"))
                    .option(Merge.onMatch, CollectionUtil.asMap("age", 33), VertexProperty.Cardinality.single)
                    .count()
                    .next();
            assertEquals(1L, result);
        } catch (Exception ex) {
            throw ex;
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldHandleInfinity() {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final GraphTraversalSource g = traversal().with(DriverRemoteConnection.using(cluster)).with("language", "gremlin-lang");
            final double result = g.inject(Double.POSITIVE_INFINITY).is(P.eq(Double.POSITIVE_INFINITY)).toList().get(0);
            assertEquals(result, Double.POSITIVE_INFINITY, 0.01);
        } catch (Exception ex) {
            throw ex;
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldSubmitMultipleQueriesWithSameConnection() throws InterruptedException, ExecutionException {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            final RequestOptions ro = RequestOptions.build().language("gremlin-groovy").create();
            final int result = client.submit("Thread.sleep(1000);1", ro).all().get().get(0).getInt();
            assertEquals(1, result);

            final AtomicInteger result2 = new AtomicInteger(-1);
            final Thread thread = new Thread(() -> {
                try {
                    result2.set(client.submit("g.inject(2)").all().get().get(0).getInt());
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            thread.start();
            thread.join();

            assertEquals(2, result2.get());
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
            client.submit("g.inject(2)").all().get();
            fail("Can't use session with HTTP");
        } catch (Exception ex) {
            final Throwable t = ExceptionUtils.getRootCause(ex);
            // assertEquals("Cannot use sessions or tx() with HttpChannelizer", t.getMessage());
            assertEquals("not implemented", t.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldDeserializeErrorWithGraphBinary() {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final GraphTraversalSource g = traversal().with(DriverRemoteConnection.using(cluster, "doesNotExist"));
            g.V().next();
            fail("Expected exception to be thrown.");
        } catch (Exception ex) {
            assert ex.getMessage().contains("Could not alias [g] to [doesNotExist] as [doesNotExist] not in the Graph or TraversalSource global bindings");
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldReportErrorWhenRequestCantBeSerialized() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final Client client = cluster.connect();

            try {
                RequestOptions ro = RequestOptions.build()
                        .language("gremlin-groovy")
                        .addParameter("r", Color.RED)
                        .create();
                client.submit("r", ro).all().get();
                fail("Should have thrown exception over bad serialization");
            } catch (Exception ex) {
                final Throwable inner = ExceptionHelper.getRootCause(ex);
                assertThat(inner, instanceOf(ResponseException.class));
                assertEquals(HttpResponseStatus.BAD_REQUEST, ((ResponseException) inner).getResponseStatusCode());
                assertTrue(ex.getMessage().contains("An error occurred during serialization of this request"));
            }

            // should not die completely just because we had a bad serialization error.  that kind of stuff happens
            // from time to time, especially in the console if you're just exploring.
            assertEquals(2, client.submit("g.inject(2)").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Ignore("not implemented in driver")
    @Test
    public void shouldProcessTraversalInterruption() {
        final Cluster cluster = TestClientFactory.build().create();

        try {
            final Client client = cluster.connect();
            final RequestOptions ro = RequestOptions.build().language("gremlin-groovy").create();
            client.submit("g.inject(1).sideEffect{Thread.sleep(5000)}", ro).all().get();
            fail("Should have timed out");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, re.getResponseStatusCode());
        } finally {
            cluster.close();
        }
    }

    @Ignore("not implemented in driver")
    @Test
    public void shouldProcessEvalInterruption() {
        final Cluster cluster = TestClientFactory.build().create();

        try {
            final Client client = cluster.connect();
            final RequestOptions ro = RequestOptions.build().language("gremlin-groovy").create();
            client.submit("Thread.sleep(5000);'done'", ro).all().get();
            fail("Should have timed out");
        } catch (Exception ex) {
            final ResponseException re = (ResponseException) ex.getCause();
            assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, re.getResponseStatusCode());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailWithBadClientSideSerialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        final RequestOptions ro = RequestOptions.build().language("gremlin-groovy").create();

        try {
            final ResultSet results = client.submit("java.awt.Color.RED", ro);

            try {
                results.all().join();
                fail("Should have thrown exception over bad serialization");
            } catch (Exception ex) {
                final Throwable inner = ExceptionHelper.getRootCause(ex);
                assertThat(inner, instanceOf(ResponseException.class));
                assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, ((ResponseException) inner).getResponseStatusCode());
            }

            // should not die completely just because we had a bad serialization error.  that kind of stuff happens
            // from time to time, especially in the console if you're just exploring.
            assertEquals(2, client.submit("g.inject(2)").all().get().get(0).getInt());
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
                final ResultSet results = client.submit("g.inject(1).math('_/0')");
                results.all().join();
                fail("Should have thrown exception over division by zero");
            } catch (Exception ex) {
                final Throwable inner = ExceptionHelper.getRootCause(ex);
                assertTrue(inner instanceof ResponseException);
                assertThat(inner.getMessage(), containsString("Division by zero"));
                assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, ((ResponseException) inner).getResponseStatusCode());
            }

            // should not die completely just because we had a bad serialization error.  that kind of stuff happens
            // from time to time, especially in the console if you're just exploring.
            assertEquals(2, client.submit("g.inject(2)").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldProcessRequestsOutOfOrder() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final RequestOptions ro = RequestOptions.build().language("gremlin-groovy").create();
        try {
            final Client client = cluster.connect();

            final ResultSet rsFive = client.submit("Thread.sleep(5000);'five'", ro);
            final ResultSet rsZero = client.submit("'zero'", ro);

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
            final ResultSet results = client.submit("g.inject(1,2,3,4,5,6,7,8,9)");
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
            final ResultSet results = client.submit("g.inject(1,2,3,4,5,6,7,8,9)");
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
            final ResultSet results = client.submit("g.inject(1,2,3,4,5,6,7,8,9)");
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
            final ResultSet results = client.submit("g.inject(1,2,3,4,5,6,7,8,9)");
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
            final ResultSet results = client.submit("g.inject(1,2,3,4,5,6,7,8,9)");
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
        final RequestOptions ro = RequestOptions.build().language("gremlin-groovy").create();
        try {

            final ResultSet results = client.submit("TinkerGraph.open().variables()", ro);

            try {
                results.all().join();
                fail();
            } catch (Exception ex) {
                final Throwable inner = ExceptionHelper.getRootCause(ex);
                assertTrue(inner instanceof ResponseException);
                assertThat(inner.getMessage(), startsWith("Error during serialization"));
                assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, ((ResponseException) inner).getResponseStatusCode());
            }

            // should not die completely just because we had a bad serialization error.  that kind of stuff happens
            // from time to time, especially in the console if you're just exploring.
            assertEquals(2, client.submit("g.inject(2)").all().get().get(0).getInt());
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
    public void shouldSubmitTraversalInGremlinLang() {
        final Cluster cluster = TestClientFactory.build().create();
        try {
            final GraphTraversalSource g = traversal().with(DriverRemoteConnection.using(cluster));

            final List result = g.with("language", "gremlin-lang").inject(null, null).inject(null, null).toList();
            assertEquals(4, result.size());
        } catch (Exception ex) {
            throw ex;
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
