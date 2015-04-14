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
import org.apache.tinkerpop.gremlin.process.traversal.T;
import org.apache.tinkerpop.gremlin.server.channel.NioChannelizer;
import org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * Integration tests for server-side settings and processing.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServerIntegrateTest extends AbstractGremlinServerIntegrationTest {

    @Rule
    public TestName name = new TestName();

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
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
            case "shouldHaveTheSessionTimeout":
                settings.processors.clear();
                final Settings.ProcessorSettings processorSettings = new Settings.ProcessorSettings();
                processorSettings.className = SessionOpProcessor.class.getCanonicalName();
                processorSettings.config = new HashMap<>();
                processorSettings.config.put(SessionOpProcessor.CONFIG_SESSION_TIMEOUT, 3000l);
                settings.processors.add(processorSettings);
                break;
        }

        return settings;
    }

    @Test
    public void shouldReturnInvalidRequestArgsWhenGremlinArgIsNotSupplied() throws Exception {
        try (SimpleClient client = new WebSocketClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL).create();
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicBoolean pass = new AtomicBoolean(false);
            client.submit(request, result -> {
                if (result.getStatus().getCode() != ResponseStatusCode.SUCCESS_TERMINATOR) {
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
    public void shouldReturnInvalidRequestArgsWhenInvalidBindingKeyIsUsed() throws Exception {
        try (SimpleClient client = new WebSocketClient()) {
            final Map<String, Object> bindings = new HashMap<>();
            bindings.put(T.id.getAccessor(), "123");
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "[1,2,3,4,5,6,7,8,9,0]")
                    .addArg(Tokens.ARGS_BINDINGS, bindings).create();
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicBoolean pass = new AtomicBoolean(false);
            client.submit(request, result -> {
                if (result.getStatus().getCode() != ResponseStatusCode.SUCCESS_TERMINATOR) {
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
    public void shouldBatchResultsByTwos() throws Exception {
        try (SimpleClient client = new WebSocketClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "[1,2,3,4,5,6,7,8,9,0]").create();

            // set the latch to six as there should be six responses when you include the terminator
            final CountDownLatch latch = new CountDownLatch(6);
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
            final CountDownLatch latch = new CountDownLatch(11);
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
            final CountDownLatch latch = new CountDownLatch(2);
            client.submit(request, r -> latch.countDown());

            assertTrue(latch.await(3000, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    public void shouldNotThrowNoSuchElementException() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        try {
            // this should return "nothing" - there should be no exception
            assertNull(client.submit("g.V().has('name','kadfjaldjfla')").one());
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
            assertTrue(re.getCause().getCause().getMessage().startsWith("Script evaluation exceeded the configured threshold of 200 ms for request"));
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
    public void shouldReceiveFailureOnBadSerialization() throws Exception {
        final Cluster cluster = Cluster.build("localhost").serializer(Serializers.JSON_V1D0).create();
        final Client client = cluster.connect();

        try {
            client.submit("def class C { def C getC(){return this}}; new C()").all().join();
            fail("Should throw an exception.");
        } catch (RuntimeException re) {
            assertTrue(re.getCause().getCause().getMessage().startsWith("Error during serialization: Direct self-reference leading to cycle (through reference chain:"));
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
            final ResultSet resultSet = result.get();
            resultSet.all().get();
            fail("Should throw an exception.");
        } catch (Exception re) {
            // can't seem to catch the server side exception - as the channel is basically closed on this error
            // can only detect a closed channel and react to that.  in some ways this is a good general piece of
            // code to have in place, but kinda stinky when you want something specific about why all went bad
            assertTrue(re.getCause().getMessage().equals("Error while processing results from channel - check client and server logs for more information"));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailOnDeadHost() throws Exception {
        final Cluster cluster = Cluster.build("localhost").serializer(Serializers.JSON_V1D0).create();
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
        }

        cluster.close();
    }

    @Test
    public void shouldEvalAndOnlyReturnTwoMessages() throws Exception {
        try (SimpleClient client = new WebSocketClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "10").create();

            // set the latch to two as there should be two responses when you include the terminator -
            // the error and the terminator
            final CountDownLatch latch = new CountDownLatch(2);
            final AtomicInteger messages = new AtomicInteger(0);
            final AtomicBoolean errorReceived = new AtomicBoolean(false);
            final AtomicBoolean terminatorReceived = new AtomicBoolean(false);
            client.submit(request, r -> {
                if (latch.getCount() == 2)
                    errorReceived.set(r.getStatus().equals(ResponseStatusCode.SUCCESS));
                else if (latch.getCount() == 1)
                    terminatorReceived.set(r.getStatus().equals(ResponseStatusCode.SUCCESS_TERMINATOR));
                latch.countDown();
                messages.incrementAndGet();
            });

            assertTrue(latch.await(1500, TimeUnit.MILLISECONDS));

            // make sure no extra message sneak in
            Thread.sleep(1000);

            assertEquals(2, messages.get());
        }
    }

    @Test
    public void shouldFailWithBadScriptEval() throws Exception {
        try (SimpleClient client = new WebSocketClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "new String().doNothingAtAllBecauseThis is a syntax error").create();

            // set the latch to two as there should be two responses when you include the terminator -
            // the error and the terminator
            final CountDownLatch latch = new CountDownLatch(2);
            final AtomicInteger messages = new AtomicInteger(0);
            final AtomicBoolean errorReceived = new AtomicBoolean(false);
            final AtomicBoolean terminatorReceived = new AtomicBoolean(false);
            client.submit(request, r -> {
                if (latch.getCount() == 2)
                    errorReceived.set(r.getStatus().equals(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION));
                else if (latch.getCount() == 1)
                    terminatorReceived.set(r.getStatus().equals(ResponseStatusCode.SUCCESS_TERMINATOR));
                latch.countDown();
                System.out.println("*******" + r);
                messages.incrementAndGet();
            });

            assertTrue(latch.await(1500, TimeUnit.MILLISECONDS));

            // make sure no extra message sneak in
            Thread.sleep(1000);

            assertEquals(2, messages.get());

        }
    }

    // todo: get this test to pass - count connection and block incoming requests.
    @Test
    @org.junit.Ignore
    public void shouldBlockWhenMaxConnectionsExceeded() throws Exception {
        final Cluster cluster = Cluster.open();
        final Client client = cluster.connect();

        try {
            final CompletableFuture<ResultSet> result = client.submitAsync("Thread.sleep(500);'test'");
            try {
                // this request should get blocked by the server
                client.submitAsync("'test-blocked'").join().one();
                fail("Request should fail because max connections are exceeded");
            } catch (Exception ex) {
                assertTrue(true);
                ex.printStackTrace();
            }

            assertEquals("test", result.get().one().getString());
        } catch (Exception re) {
            fail("Should not have an exception here");
        } finally {
            cluster.close();
        }
    }
}
