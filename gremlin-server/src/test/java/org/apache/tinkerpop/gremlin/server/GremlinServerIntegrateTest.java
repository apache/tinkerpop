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

import java.io.File;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.driver.simple.NioClient;
import org.apache.tinkerpop.gremlin.driver.simple.SimpleClient;
import org.apache.tinkerpop.gremlin.driver.simple.WebSocketClient;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.server.channel.NioChannelizer;
import org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.util.Log4jRecordingAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeThat;

/**
 * Integration tests for server-side settings and processing.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServerIntegrateTest extends AbstractGremlinServerIntegrationTest {

    private Log4jRecordingAppender recordingAppender = null;

    @Before
    public void setupForEachTest() {
        recordingAppender = new Log4jRecordingAppender();
        final Logger rootLogger = Logger.getRootLogger();
        rootLogger.addAppender(recordingAppender);
    }

    @After
    public void teardownForEachTest() {
        final Logger rootLogger = Logger.getRootLogger();
        rootLogger.removeAppender(recordingAppender);
    }

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldRespectHighWaterMarkSettingAndSucceed":
                settings.writeBufferHighWaterMark = 64;
                settings.writeBufferLowWaterMark = 32;
                break;
            case "shouldReceiveFailureTimeOutOnScriptEval":
                settings.scriptEvaluationTimeout = 200;
                break;
            case "shouldReceiveFailureTimeOutOnTotalSerialization":
                settings.serializedResponseTimeout = 1;
                break;
            case "shouldBlockRequestWhenTooBig":
                settings.maxContentLength = 1024;
                break;
            case "shouldBatchResultsByTwos":
                settings.resultIterationBatchSize = 2;
                break;
            case "shouldWorkOverNioTransport":
                settings.channelizer = NioChannelizer.class.getName();
                break;
            case "shouldEnableSsl":
            case "shouldEnableSslButFailIfClientConnectsWithoutIt":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                break;
            case "shouldStartWithDefaultSettings":
                return new Settings();
            case "shouldHaveTheSessionTimeout":
                settings.processors.clear();
                final Settings.ProcessorSettings processorSettings = new Settings.ProcessorSettings();
                processorSettings.className = SessionOpProcessor.class.getCanonicalName();
                processorSettings.config = new HashMap<>();
                processorSettings.config.put(SessionOpProcessor.CONFIG_SESSION_TIMEOUT, 3000l);
                settings.processors.add(processorSettings);
                break;
            case "shouldExecuteInSessionAndSessionlessWithoutOpeningTransactionWithSingleClient":
                deleteDirectory(new File("/tmp/neo4j"));
                settings.graphs.put("graph", "conf/neo4j-empty.properties");
                break;
        }

        return settings;
    }

    @Test
    public void shouldStartWithDefaultSettings() {
        // just quickly validate that results are returning given defaults. no graphs are config'd with defaults
        // so just eval a groovy script.
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        final ResultSet results = client.submit("[1,2,3,4,5,6,7,8,9]");
        final AtomicInteger counter = new AtomicInteger(0);
        results.stream().map(i -> i.get(Integer.class) * 2).forEach(i -> assertEquals(counter.incrementAndGet() * 2, Integer.parseInt(i.toString())));

        cluster.close();
    }

    @Test
    public void shouldEnableSsl() {
        final Cluster cluster = Cluster.build().enableSsl(true).create();
        final Client client = cluster.connect();

        try {
            // this should return "nothing" - there should be no exception
            assertEquals("test", client.submit("'test'").one().getString());
        } finally {
            cluster.close();
        }
    }

    @org.junit.Ignore("This test hangs - not sure why")
    @Test
    public void shouldEnableSslButFailIfClientConnectsWithoutIt() {
        // todo: need to get this to pass somehow - should just error out.
        final Cluster cluster = Cluster.build().enableSsl(false).create();
        final Client client = cluster.connect();

        try {
            // this should return "nothing" - there should be no exception
            assertEquals("test", client.submit("'test'").one().getString());
        } catch(Exception x) {
            x.printStackTrace();
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldRespectHighWaterMarkSettingAndSucceed() throws Exception {
        // the highwatermark should get exceeded on the server and thus pause the writes, but have no problem catching
        // itself up - this is a tricky tests to get passing on all environments so this assumption will deny the
        // test for most cases
        assumeThat("Set the 'assertNonDeterministic' property to true to execute this test",
                System.getProperty("assertNonDeterministic"), is("true"));

        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        try {
            final int resultCountToGenerate = 1000;
            final int batchSize = 3;
            final String fatty = IntStream.range(0, 175).mapToObj(String::valueOf).collect(Collectors.joining());
            final String fattyX = "['" + fatty + "'] * " + resultCountToGenerate;

            // don't allow the thread to proceed until all results are accounted for
            final CountDownLatch latch = new CountDownLatch(resultCountToGenerate);
            final AtomicBoolean expected = new AtomicBoolean(false);
            final AtomicBoolean faulty = new AtomicBoolean(false);
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_BATCH_SIZE, batchSize)
                    .addArg(Tokens.ARGS_GREMLIN, fattyX).create();

            client.submitAsync(request).thenAcceptAsync(r -> {
                r.stream().forEach(item -> {
                    try {
                        final String aFattyResult = item.getString();
                        expected.set(aFattyResult.equals(fatty));
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        faulty.set(true);
                    } finally {
                        latch.countDown();
                    }
                });
            });

            assertTrue(latch.await(30000, TimeUnit.MILLISECONDS));
            assertEquals(0, latch.getCount());
            assertFalse(faulty.get());
            assertTrue(expected.get());

            assertTrue(recordingAppender.getMessages().stream().anyMatch(m -> m.contains("Pausing response writing as writeBufferHighWaterMark exceeded on")));
        } catch (Exception ex) {
            fail("Shouldn't have tossed an exception");
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldReturnInvalidRequestArgsWhenGremlinArgIsNotSupplied() throws Exception {
        try (SimpleClient client = new WebSocketClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL).create();
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicBoolean pass = new AtomicBoolean(false);
            client.submit(request, result -> {
                if (result.getStatus().getCode() != ResponseStatusCode.PARTIAL_CONTENT) {
                    pass.set(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS == result.getStatus().getCode());
                    latch.countDown();
                }
            });

            if (!latch.await(300, TimeUnit.MILLISECONDS))
                fail("Request should have returned error, but instead timed out");
            assertTrue(pass.get());
        }
    }

    @Test
    public void shouldReturnInvalidRequestArgsWhenInvalidReservedBindingKeyIsUsed() throws Exception {
        try (SimpleClient client = new WebSocketClient()) {
            final Map<String, Object> bindings = new HashMap<>();
            bindings.put(T.id.getAccessor(), "123");
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "[1,2,3,4,5,6,7,8,9,0]")
                    .addArg(Tokens.ARGS_BINDINGS, bindings).create();
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicBoolean pass = new AtomicBoolean(false);
            client.submit(request, result -> {
                if (result.getStatus().getCode() != ResponseStatusCode.PARTIAL_CONTENT) {
                    pass.set(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS == result.getStatus().getCode());
                    latch.countDown();
                }
            });

            if (!latch.await(3000, TimeUnit.MILLISECONDS))
                fail("Request should have returned error, but instead timed out");
            assertTrue(pass.get());
        }
    }

    @Test
    public void shouldReturnInvalidRequestArgsWhenInvalidTypeBindingKeyIsUsed() throws Exception {
        try (SimpleClient client = new WebSocketClient()) {
            final Map<Object, Object> bindings = new HashMap<>();
            bindings.put(1, "123");
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "[1,2,3,4,5,6,7,8,9,0]")
                    .addArg(Tokens.ARGS_BINDINGS, bindings).create();
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicBoolean pass = new AtomicBoolean(false);
            client.submit(request, result -> {
                if (result.getStatus().getCode() != ResponseStatusCode.PARTIAL_CONTENT) {
                    pass.set(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS == result.getStatus().getCode());
                    latch.countDown();
                }
            });

            if (!latch.await(3000, TimeUnit.MILLISECONDS))
                fail("Request should have returned error, but instead timed out");
            assertTrue(pass.get());
        }
    }

    @Test
    public void shouldReturnInvalidRequestArgsWhenInvalidNullBindingKeyIsUsed() throws Exception {
        try (SimpleClient client = new WebSocketClient()) {
            final Map<String, Object> bindings = new HashMap<>();
            bindings.put(null, "123");
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "[1,2,3,4,5,6,7,8,9,0]")
                    .addArg(Tokens.ARGS_BINDINGS, bindings).create();
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicBoolean pass = new AtomicBoolean(false);
            client.submit(request, result -> {
                if (result.getStatus().getCode() != ResponseStatusCode.PARTIAL_CONTENT) {
                    pass.set(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS == result.getStatus().getCode());
                    latch.countDown();
                }
            });

            if (!latch.await(3000, TimeUnit.MILLISECONDS))
                fail("Request should have returned error, but instead timed out");
            assertTrue(pass.get());
        }
    }

    @Test
    public void shouldBatchResultsByTwos() throws Exception {
        try (SimpleClient client = new WebSocketClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "[1,2,3,4,5,6,7,8,9,0]").create();

            // set the latch to six as there should be six responses when you include the terminator
            final CountDownLatch latch = new CountDownLatch(5);
            client.submit(request, r -> latch.countDown());

            assertTrue(latch.await(1500, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    public void shouldBatchResultsByOnesByOverridingFromClientSide() throws Exception {
        try (SimpleClient client = new WebSocketClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "[1,2,3,4,5,6,7,8,9,0]")
                    .addArg(Tokens.ARGS_BATCH_SIZE, 1).create();

            // should be 11 responses when you include the terminator
            final CountDownLatch latch = new CountDownLatch(10);
            client.submit(request, r -> latch.countDown());

            assertTrue(latch.await(1500, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    public void shouldWorkOverNioTransport() throws Exception {
        try (SimpleClient client = new NioClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "[1,2,3,4,5,6,7,8,9,0]").create();

            // should be 2 responses when you include the terminator
            final CountDownLatch latch = new CountDownLatch(1);
            client.submit(request, r -> latch.countDown());

            assertTrue(latch.await(30000, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    public void shouldNotThrowNoSuchElementException() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        try {
            // this should return "nothing" - there should be no exception
            assertNull(client.submit("g.V().has('name','kadfjaldjfla')").one());
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldReceiveFailureTimeOutOnScriptEval() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        try {
            client.submit("Thread.sleep(3000);'some-stuff-that-should not return'").all().join();
            fail("Should throw an exception.");
        } catch (RuntimeException re) {
            assertTrue(ExceptionUtils.getRootCause(re).getMessage().startsWith("Script evaluation exceeded the configured threshold of 200 ms for request"));

            // validate that we can still send messages to the server
            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldReceiveFailureTimeOutOnTotalSerialization() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        try {
            client.submit("(0..<100000)").all().join();
            fail("Should throw an exception.");
        } catch (RuntimeException re) {
            assertTrue(re.getCause().getMessage().endsWith("Serialization of the entire response exceeded the serializeResponseTimeout setting"));

            // validate that we can still send messages to the server
            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldLoadInitScript() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();
        try {
            assertEquals(2, client.submit("addItUp(1,1)").all().join().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldGarbageCollectPhantomButNotHard() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        assertEquals(2, client.submit("addItUp(1,1)").all().join().get(0).getInt());
        assertEquals(0, client.submit("def subtract(x,y){x-y};subtract(1,1)").all().join().get(0).getInt());
        assertEquals(0, client.submit("subtract(1,1)").all().join().get(0).getInt());

        final Map<String, Object> bindings = new HashMap<>();
        bindings.put(GremlinGroovyScriptEngine.KEY_REFERENCE_TYPE, GremlinGroovyScriptEngine.REFERENCE_TYPE_PHANTOM);
        assertEquals(4, client.submit("def multiply(x,y){x*y};multiply(2,2)", bindings).all().join().get(0).getInt());

        try {
            client.submit("multiply(2,2)").all().join().get(0).getInt();
            fail("Should throw an exception since reference is phantom.");
        } catch (RuntimeException ignored) {

        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldReceiveFailureOnBadGraphSONSerialization() throws Exception {
        final Cluster cluster = Cluster.build("localhost").serializer(Serializers.GRAPHSON_V1D0).create();
        final Client client = cluster.connect();

        try {
            client.submit("def class C { def C getC(){return this}}; new C()").all().join();
            fail("Should throw an exception.");
        } catch (RuntimeException re) {
            assertTrue(re.getCause().getCause().getMessage().startsWith("Error during serialization: Direct self-reference leading to cycle (through reference chain:"));

            // validate that we can still send messages to the server
            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldReceiveFailureOnBadGryoSerialization() throws Exception {
        final Cluster cluster = Cluster.build("localhost").serializer(Serializers.GRYO_V1D0).create();
        final Client client = cluster.connect();

        try {
            client.submit("java.awt.Color.RED").all().join();
            fail("Should throw an exception.");
        } catch (RuntimeException re) {
            assertTrue(re.getCause().getCause().getMessage().startsWith("Error during serialization: Class is not registered: java.awt.Color"));

            // validate that we can still send messages to the server
            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldBlockRequestWhenTooBig() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        try {
            final String fatty = IntStream.range(0, 1024).mapToObj(String::valueOf).collect(Collectors.joining());
            final CompletableFuture<ResultSet> result = client.submitAsync("'" + fatty + "';'test'");
            final ResultSet resultSet = result.get(10000, TimeUnit.MILLISECONDS);
            resultSet.all().get(10000, TimeUnit.MILLISECONDS);
            fail("Should throw an exception.");
        } catch (TimeoutException te) {
            // the request should not have timed-out - the connection should have been reset, but it seems that
            // timeout seems to occur as well on some systems (it's not clear why).  however, the nature of this
            // test is to ensure that the script isn't processed if it exceeds a certain size, so in this sense
            // it seems ok to pass in this case.
        } catch (Exception re) {
            final Throwable root = ExceptionUtils.getRootCause(re);
            assertEquals("Connection reset by peer", root.getMessage());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailOnDeadHost() throws Exception {
        final Cluster cluster = Cluster.build("localhost").create();
        final Client client = cluster.connect();

        // ensure that connection to server is good
        assertEquals(2, client.submit("1+1").all().join().get(0).getInt());

        // kill the server which will make the client mark the host as unavailable
        this.stopServer();

        try {
            // try to re-issue a request now that the server is down
            client.submit("1+1").all().join();
            fail();
        } catch (RuntimeException re) {
            assertTrue(re.getCause().getCause() instanceof ClosedChannelException);
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldHaveTheSessionTimeout() throws Exception {
        final Cluster cluster = Cluster.build().create();
        final Client client = cluster.connect(name.getMethodName());

        final ResultSet results1 = client.submit("x = [1,2,3,4,5,6,7,8,9]");
        final AtomicInteger counter = new AtomicInteger(0);
        results1.stream().map(i -> i.get(Integer.class) * 2).forEach(i -> assertEquals(counter.incrementAndGet() * 2, Integer.parseInt(i.toString())));

        final ResultSet results2 = client.submit("x[0]+1");
        assertEquals(2, results2.all().get().get(0).getInt());

        // session times out in 3 seconds
        Thread.sleep(3500);

        try {
            client.submit("x[1]+2").all().get();
            fail("Session should be dead");
        } catch (Exception ex) {
            final Exception cause = (Exception) ex.getCause().getCause();
            assertTrue(cause instanceof ResponseException);
            assertEquals(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION, ((ResponseException) cause).getResponseStatusCode());

            // validate that we can still send messages to the server
            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEvalAndReturnSuccessOnlyNoPartialContent() throws Exception {
        try (SimpleClient client = new WebSocketClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "10").create();

            // set the latch to two as there should be two responses when you include the terminator -
            // the error and the terminator
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicInteger messages = new AtomicInteger(0);
            final AtomicBoolean errorReceived = new AtomicBoolean(false);
            client.submit(request, r -> {
                errorReceived.set(r.getStatus().equals(ResponseStatusCode.SUCCESS));
                latch.countDown();
                messages.incrementAndGet();
            });

            assertTrue(latch.await(1500, TimeUnit.MILLISECONDS));

            // make sure no extra message sneak in
            Thread.sleep(1000);

            assertEquals(1, messages.get());
        }
    }

    @Test
    public void shouldFailWithBadScriptEval() throws Exception {
        try (SimpleClient client = new WebSocketClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "new String().doNothingAtAllBecauseThis is a syntax error").create();

            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicInteger messages = new AtomicInteger(0);
            final AtomicBoolean errorReceived = new AtomicBoolean(false);
            client.submit(request, r -> {
                errorReceived.set(r.getStatus().equals(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION));
                latch.countDown();
                messages.incrementAndGet();
            });

            assertTrue(latch.await(1500, TimeUnit.MILLISECONDS));

            // make sure no extra message sneak in
            Thread.sleep(1000);

            assertEquals(1, messages.get());
        }
    }
    
    @Test
    public void shouldExecuteInSessionAndSessionlessWithoutOpeningTransactionWithSingleClient() throws Exception {
        assumeNeo4jIsPresent();
        
        final SimpleClient client = new WebSocketClient();
        
        //open a transaction, create a vertex, commit
        final CountDownLatch latch = new CountDownLatch(1);
        final RequestMessage OpenRequest = RequestMessage.build(Tokens.OPS_EVAL)
                .processor("session")
                .addArg(Tokens.ARGS_SESSION, name.getMethodName())
                .addArg(Tokens.ARGS_GREMLIN, "graph.tx().open()")
                .create();
        client.submit(OpenRequest, (r) -> {
            latch.countDown();
        });
        assertTrue(latch.await(1500, TimeUnit.MILLISECONDS));
        
        final CountDownLatch latch2 = new CountDownLatch(1);
        final RequestMessage AddRequest = RequestMessage.build(Tokens.OPS_EVAL)
                .processor("session")
                .addArg(Tokens.ARGS_SESSION, name.getMethodName())
                .addArg(Tokens.ARGS_GREMLIN, "v=graph.addVertex(\"name\",\"stephen\")")
                .create();
        client.submit(AddRequest, (r) -> {
            latch2.countDown();
        });
        assertTrue(latch2.await(1500, TimeUnit.MILLISECONDS));
        
        final CountDownLatch latch3 = new CountDownLatch(1);
        final RequestMessage CommitRequest = RequestMessage.build(Tokens.OPS_EVAL)
                .processor("session")
                .addArg(Tokens.ARGS_SESSION, name.getMethodName())
                .addArg(Tokens.ARGS_GREMLIN, "graph.tx().commit()")
                .create();
        client.submit(CommitRequest, (r) -> {
            latch3.countDown();
            
        });
        assertTrue(latch3.await(1500, TimeUnit.MILLISECONDS));
        
        // Check to see if the transaction is closed.
        final CountDownLatch latch4 = new CountDownLatch(1);
        final AtomicBoolean isOpen = new AtomicBoolean(false);
        final RequestMessage CheckRequest = RequestMessage.build(Tokens.OPS_EVAL)
                .processor("session")
                .addArg(Tokens.ARGS_SESSION, name.getMethodName())
                .addArg(Tokens.ARGS_GREMLIN, "graph.tx().isOpen()")
                .create();
        client.submit(CheckRequest, (r) -> {
            ArrayList<Boolean> response = (ArrayList) r.getResult().getData();
            isOpen.set(response.get(0));
            latch4.countDown();
        });
        assertTrue(latch4.await(1500, TimeUnit.MILLISECONDS));
        
        // make sure no extra message sneak in
        Thread.sleep(1000);
        
        assertTrue("Transaction should be closed", !isOpen.get());
        
        //lets run a sessionless read
        final CountDownLatch latch5 = new CountDownLatch(1);
        final RequestMessage sessionlessRequest = RequestMessage.build(Tokens.OPS_EVAL)
                .addArg(Tokens.ARGS_GREMLIN, "graph.traversal().V()")
                .create();
        client.submit(sessionlessRequest, (r) -> {
            latch5.countDown();
        });
        assertTrue(latch5.await(1500, TimeUnit.MILLISECONDS));
        
        // Check to see if the transaction is still closed.
        final CountDownLatch latch6 = new CountDownLatch(1);
        final AtomicBoolean isStillOpen = new AtomicBoolean(false);
        final RequestMessage CheckAgainRequest = RequestMessage.build(Tokens.OPS_EVAL)
                .processor("session")
                .addArg(Tokens.ARGS_SESSION, name.getMethodName())
                .addArg(Tokens.ARGS_GREMLIN, "graph.tx().isOpen()")
                .create();
        client.submit(CheckAgainRequest, (r) -> {
            ArrayList<Boolean> response = (ArrayList) r.getResult().getData();
            isStillOpen.set(response.get(0));
            latch6.countDown();
        });
        assertTrue(latch6.await(1500, TimeUnit.MILLISECONDS));
        
        // make sure no extra message sneak in
        Thread.sleep(1000);
        
        assertTrue("Transaction should still be closed", !isStillOpen.get());
    }

    @Test
    public void shouldStillSupportDeprecatedRebindingsParameterOnServer() throws Exception {
        // this test can be removed when the rebindings arg is removed
        try (SimpleClient client = new WebSocketClient()) {
            final Map<String,String> rebindings = new HashMap<>();
            rebindings.put("xyz", "graph");
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "xyz.addVertex('name','jason')")
                    .addArg(Tokens.ARGS_REBINDINGS, rebindings).create();
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicBoolean pass = new AtomicBoolean(false);
            client.submit(request, result -> {
                final List<Object> results = (List<Object>) result.getResult().getData();
                final DetachedVertex v = (DetachedVertex) results.get(0);
                pass.set(ResponseStatusCode.SUCCESS == result.getStatus().getCode() && v.value("name").equals("jason"));
                latch.countDown();
            });

            if (!latch.await(300, TimeUnit.MILLISECONDS)) fail("Request should have returned a response");

            assertTrue(pass.get());
        }
    }
}
