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

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.driver.simple.SimpleClient;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyCompilerGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.SimpleSandboxExtension;
import org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin;
import org.apache.tinkerpop.gremlin.structure.RemoteGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.server.handler.OpSelectorHandler;
import org.apache.tinkerpop.gremlin.server.op.AbstractEvalOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.Log4jRecordingAppender;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.tinkerpop.gremlin.driver.Tokens.ARGS_EVAL_TIMEOUT;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyCompilerGremlinPlugin.Compilation.COMPILE_STATIC;
import static org.apache.tinkerpop.gremlin.process.remote.RemoteConnection.GREMLIN_REMOTE;
import static org.apache.tinkerpop.gremlin.process.remote.RemoteConnection.GREMLIN_REMOTE_CONNECTION_CLASS;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for server-side settings and processing.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServerIntegrateTest extends AbstractGremlinServerIntegrationTest {

    private Level previousLogLevel;

    private Log4jRecordingAppender recordingAppender = null;
    private final Supplier<Graph> graphGetter = () -> server.getServerGremlinExecutor().getGraphManager().getGraph("graph");
    private final Configuration conf = new BaseConfiguration() {{
        setProperty(Graph.GRAPH, RemoteGraph.class.getName());
        setProperty(GREMLIN_REMOTE_CONNECTION_CLASS, DriverRemoteConnection.class.getName());
        setProperty(DriverRemoteConnection.GREMLIN_REMOTE_DRIVER_SOURCENAME, "g");
        setProperty(GREMLIN_REMOTE + "attachment", graphGetter);
        setProperty("clusterConfiguration.port", TestClientFactory.PORT);
        setProperty("clusterConfiguration.hosts", "localhost");
    }};

    @Before
    public void setupForEachTest() {
        recordingAppender = new Log4jRecordingAppender();
        final Logger rootLogger = Logger.getRootLogger();

        if (name.getMethodName().equals("shouldPingChannelIfClientDies") ||
                name.getMethodName().equals("shouldCloseChannelIfClientDoesntRespond")) {
            final org.apache.log4j.Logger webSocketClientHandlerLogger = org.apache.log4j.Logger.getLogger(OpSelectorHandler.class);
            previousLogLevel = webSocketClientHandlerLogger.getLevel();
            webSocketClientHandlerLogger.setLevel(Level.INFO);
        }

        rootLogger.addAppender(recordingAppender);
    }

    @After
    public void teardownForEachTest() {
        final Logger rootLogger = Logger.getRootLogger();

        if (name.getMethodName().equals("shouldPingChannelIfClientDies")||
                name.getMethodName().equals("shouldCloseChannelIfClientDoesntRespond")) {
            final org.apache.log4j.Logger webSocketClientHandlerLogger = org.apache.log4j.Logger.getLogger(OpSelectorHandler.class);
            webSocketClientHandlerLogger.setLevel(previousLogLevel);
        }

        rootLogger.removeAppender(recordingAppender);
    }

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldProvideBetterExceptionForMethodCodeTooLarge":
                settings.maxContentLength = 4096000;
                final Settings.ProcessorSettings processorSettingsBig = new Settings.ProcessorSettings();
                processorSettingsBig.className = StandardOpProcessor.class.getName();
                processorSettingsBig.config = new HashMap<String,Object>() {{
                    put(AbstractEvalOpProcessor.CONFIG_MAX_PARAMETERS, Integer.MAX_VALUE);
                }};
                settings.processors.clear();
                settings.processors.add(processorSettingsBig);
                break;
            case "shouldRespectHighWaterMarkSettingAndSucceed":
                settings.writeBufferHighWaterMark = 64;
                settings.writeBufferLowWaterMark = 32;
                break;
            case "shouldReceiveFailureTimeOutOnScriptEval":
                settings.evaluationTimeout = 1000;
                break;
            case "shouldBlockRequestWhenTooBig":
                settings.maxContentLength = 1024;
                break;
            case "shouldBatchResultsByTwos":
                settings.resultIterationBatchSize = 2;
                break;
            case "shouldUseSimpleSandbox":
                settings.scriptEngines.get("gremlin-groovy").plugins.put(GroovyCompilerGremlinPlugin.class.getName(), getScriptEngineConfForSimpleSandbox());
                // remove the script because it isn't used in the test but also because it's not CompileStatic ready
                settings.scriptEngines.get("gremlin-groovy").plugins.remove(ScriptFileGremlinPlugin.class.getName());
                break;
            case "shouldUseInterpreterMode":
                settings.scriptEngines.get("gremlin-groovy").plugins.put(GroovyCompilerGremlinPlugin.class.getName(), getScriptEngineConfForInterpreterMode());
                break;
            case "shouldReceiveFailureTimeOutOnScriptEvalOfOutOfControlLoop":
                settings.scriptEngines.get("gremlin-groovy").plugins.put(GroovyCompilerGremlinPlugin.class.getName(), getScriptEngineConfForTimedInterrupt());
                break;
            case "shouldUseBaseScript":
                settings.scriptEngines.get("gremlin-groovy").plugins.put(GroovyCompilerGremlinPlugin.class.getName(), getScriptEngineConfForBaseScript());
                settings.scriptEngines.get("gremlin-groovy").config = getScriptEngineConfForBaseScript();
                break;
            case "shouldReturnInvalidRequestArgsWhenBindingCountExceedsAllowable":
                final Settings.ProcessorSettings processorSettingsSmall = new Settings.ProcessorSettings();
                processorSettingsSmall.className = StandardOpProcessor.class.getName();
                processorSettingsSmall.config = new HashMap<String,Object>() {{
                    put(AbstractEvalOpProcessor.CONFIG_MAX_PARAMETERS, 1);
                }};
                settings.processors.clear();
                settings.processors.add(processorSettingsSmall);
                break;
            case "shouldTimeOutRemoteTraversal":
                settings.evaluationTimeout = 500;
                break;
            case "shouldPingChannelIfClientDies":
                settings.keepAliveInterval = 1000;
                break;
            case "shouldCloseChannelIfClientDoesntRespond":
                settings.idleConnectionTimeout = 1000;
                break;
            default:
                break;
        }

        return settings;
    }

    private static Map<String, Object> getScriptEngineConfForSimpleSandbox() {
        final Map<String,Object> scriptEngineConf = new HashMap<>();
        scriptEngineConf.put("compilation", COMPILE_STATIC.name());
        scriptEngineConf.put("extensions", SimpleSandboxExtension.class.getName());
        return scriptEngineConf;
    }

    private static Map<String, Object> getScriptEngineConfForTimedInterrupt() {
        final Map<String,Object> scriptEngineConf = new HashMap<>();
        scriptEngineConf.put("timedInterrupt", 1000);
        return scriptEngineConf;
    }

    private static Map<String, Object> getScriptEngineConfForInterpreterMode() {
        final Map<String,Object> scriptEngineConf = new HashMap<>();
        scriptEngineConf.put("enableInterpreterMode", true);
        return scriptEngineConf;
    }

    private static Map<String, Object> getScriptEngineConfForBaseScript() {
        final Map<String,Object> scriptEngineConf = new HashMap<>();
        final Map<String,Object> properties = new HashMap<>();
        properties.put("ScriptBaseClass", BaseScriptForTesting.class.getName());
        scriptEngineConf.put("compilerConfigurationOptions", properties);
        return scriptEngineConf;
    }

    @Test
    public void shouldScriptEvaluationErrorForRemoteTraversal() throws Exception {
        final GraphTraversalSource g = traversal().withRemote(conf);

        try {
            // tests bad lambda
            g.inject(1).sideEffect(Lambda.consumer("(")).iterate();
            fail("This traversal should not have executed since lambda can't be compiled");
        } catch (Exception ex) {
            final Throwable t = ex.getCause();
            assertThat(t, instanceOf(ResponseException.class));
            assertEquals(ResponseStatusCode.SERVER_ERROR_EVALUATION, ((ResponseException) t).getResponseStatusCode());
        }

        // make a graph with a cycle in it to force a long run traversal
        graphGetter.get().traversal().addV("person").as("p").addE("self").to("p").iterate();

        try {
            // tests an "unending" traversal
            g.V().repeat(__.out()).until(__.outE().count().is(0)).iterate();
            fail("This traversal should have timed out");
        } catch (Exception ex) {
            final Throwable t = ex.getCause();
            assertThat(t, instanceOf(ResponseException.class));
            assertEquals(ResponseStatusCode.SERVER_ERROR_TIMEOUT, ((ResponseException) t).getResponseStatusCode());
        }
    }

    @Test
    public void shouldCloseChannelIfClientDoesntRespond() throws Exception {
        final SimpleClient client = TestClientFactory.createWebSocketClient();
        client.submit("1+1");

        // since we do nothing for 2 seconds and the time limit for timeout on the server is 1 second, the server
        // will autoclose the channel
        Thread.sleep(2000);

        assertThat(recordingAppender.logContainsAny(".*Closing channel - client is disconnected after idle period of .*$"), is(true));

        client.close();
    }

    @Test
    public void shouldPingChannelIfClientDies() throws Exception {
        final Client client = TestClientFactory.build().maxConnectionPoolSize(1).minConnectionPoolSize(1).keepAliveInterval(0).create().connect();
        client.submit("1+1").all().get();

        // since we do nothing for 3 seconds and the time limit for ping is 1 second we should get *about* 3 pings -
        // i don't think the assertion needs to be too accurate. just need to make sure there's a ping message out
        // there record
        Thread.sleep(3000);

        client.close();

        // stop the server to be sure that logs flush
        stopServer();

        assertThat(recordingAppender.logContainsAny(".*Checking channel - sending ping to client after idle period of .*$"), is(true));
    }

    @Test
    public void shouldTimeOutRemoteTraversal() throws Exception {
        final GraphTraversalSource g = traversal().withRemote(conf);

        try {
            // tests sleeping thread
            g.inject(1).sideEffect(Lambda.consumer("Thread.sleep(10000)")).iterate();
            fail("This traversal should have timed out");
        } catch (Exception ex) {
            final Throwable t = ex.getCause();
            assertThat(t, instanceOf(ResponseException.class));
            assertEquals(ResponseStatusCode.SERVER_ERROR_TIMEOUT, ((ResponseException) t).getResponseStatusCode());
        }

        // make a graph with a cycle in it to force a long run traversal
        graphGetter.get().traversal().addV("person").as("p").addE("self").to("p").iterate();

        try {
            // tests an "unending" traversal
            g.V().repeat(__.out()).until(__.outE().count().is(0)).iterate();
            fail("This traversal should have timed out");
        } catch (Exception ex) {
            final Throwable t = ex.getCause();
            assertThat(t, instanceOf(ResponseException.class));
            assertEquals(ResponseStatusCode.SERVER_ERROR_TIMEOUT, ((ResponseException) t).getResponseStatusCode());
        }
    }

    @Test
    public void shouldTimeOutRemoteTraversalWithPerRequestOption() {
        final GraphTraversalSource g = traversal().withRemote(conf);

        try {
            // tests sleeping thread
            g.with(ARGS_EVAL_TIMEOUT, 500L).inject(1).sideEffect(Lambda.consumer("Thread.sleep(10000)")).iterate();
            fail("This traversal should have timed out");
        } catch (Exception ex) {
            final Throwable t = ex.getCause();
            assertThat(t, instanceOf(ResponseException.class));
            assertEquals(ResponseStatusCode.SERVER_ERROR_TIMEOUT, ((ResponseException) t).getResponseStatusCode());
        }

        // make a graph with a cycle in it to force a long run traversal
        graphGetter.get().traversal().addV("person").as("p").addE("self").to("p").iterate();

        try {
            // tests an "unending" traversal
            g.with(ARGS_EVAL_TIMEOUT, 500L).V().repeat(__.out()).until(__.outE().count().is(0)).iterate();
            fail("This traversal should have timed out");
        } catch (Exception ex) {
            final Throwable t = ex.getCause();
            assertThat(t, instanceOf(ResponseException.class));
            assertEquals(ResponseStatusCode.SERVER_ERROR_TIMEOUT, ((ResponseException) t).getResponseStatusCode());
        }
    }

    @Test
    public void shouldProduceProperExceptionOnTimeout() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect(name.getMethodName());

        boolean success = false;
        // Run a short test script a few times with progressively longer timeouts.
        // Each submissions should either succeed or fail with a timeout.
        // Note: the range of timeouts is intended to cover the case when the script finishes at about the
        // same time when the timeout occurs. In this situation either a timeout response or a successful
        // response is acceptable, however no other processing errors should occur.
        // Note: the timeout of 30 ms is generally sufficient for running a simple groovy script, so using longer
        // timeouts are not likely to results in a success/timeout response collision, which is the purpose
        // of this test.
        // Note: this test may have a false negative result, but a failure  would indicate a real problem.
        for(int i = 0; i < 30; i++) {
            int timeout = 1 + i;
            overrideEvaluationTimeout(timeout);

            try {
                client.submit("x = 1 + 1").all().get().get(0).getInt();
                success = true;
            } catch (Exception ex) {
                final Throwable t = ex.getCause();
                assertThat("Unexpected exception with script evaluation timeout: " + timeout, t, instanceOf(ResponseException.class));
                assertEquals(ResponseStatusCode.SERVER_ERROR_TIMEOUT, ((ResponseException) t).getResponseStatusCode());
            }
        }

        assertTrue("Some script submissions should succeed", success);

        cluster.close();
    }

    @Test
    public void shouldUseBaseScript() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect(name.getMethodName());

        assertEquals("hello, stephen", client.submit("hello('stephen')").all().get().get(0).getString());

        cluster.close();
    }

    @Test
    public void shouldUseInterpreterMode() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect(name.getMethodName());

        client.submit("def subtractAway(x,y){x-y};[]").all().get();
        client.submit("multiplyIt = { x,y -> x * y};[]").all().get();

        assertEquals(2, client.submit("x = 1 + 1").all().get().get(0).getInt());
        assertEquals(3, client.submit("int y = x + 1").all().get().get(0).getInt());
        assertEquals(5, client.submit("def z = x + y").all().get().get(0).getInt());

        final Map<String,Object> m = new HashMap<>();
        m.put("x", 10);
        assertEquals(-5, client.submit("z - x", m).all().get().get(0).getInt());
        assertEquals(15, client.submit("addItUp(x,z)", m).all().get().get(0).getInt());
        assertEquals(5, client.submit("subtractAway(x,z)", m).all().get().get(0).getInt());
        assertEquals(50, client.submit("multiplyIt(x,z)", m).all().get().get(0).getInt());

        cluster.close();
    }

    @Test
    public void shouldNotUseInterpreterMode() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect(name.getMethodName());

        client.submit("def subtractAway(x,y){x-y};[]").all().get();
        client.submit("multiplyIt = { x,y -> x * y};[]").all().get();

        assertEquals(2, client.submit("x = 1 + 1").all().get().get(0).getInt());
        assertEquals(3, client.submit("y = x + 1").all().get().get(0).getInt());
        assertEquals(5, client.submit("z = x + y").all().get().get(0).getInt());

        final Map<String,Object> m = new HashMap<>();
        m.put("x", 10);
        assertEquals(-5, client.submit("z - x", m).all().get().get(0).getInt());
        assertEquals(15, client.submit("addItUp(x,z)", m).all().get().get(0).getInt());
        assertEquals(5, client.submit("subtractAway(x,z)", m).all().get().get(0).getInt());
        assertEquals(50, client.submit("multiplyIt(x,z)", m).all().get().get(0).getInt());

        cluster.close();
    }

    @Test
    public void shouldUseSimpleSandbox() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        assertEquals(2, client.submit("1+1").all().get().get(0).getInt());

        try {
            // this should return "nothing" - there should be no exception
            client.submit("java.lang.System.exit(0)").all().get();
            fail("The above should not have executed in any successful way as sandboxing is enabled");
        } catch (Exception ex) {
            assertThat(ex.getCause().getMessage(), containsString("[Static type checking] - Not authorized to call this method: java.lang.System#exit(int)"));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldRespectHighWaterMarkSettingAndSucceed() throws Exception {
        // the highwatermark should get exceeded on the server and thus pause the writes, but have no problem catching
        // itself up - this is a tricky tests to get passing on all environments so this assumption will deny the
        // test for most cases
        TestHelper.assumeNonDeterministic();

        final Cluster cluster = TestClientFactory.open();
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

            assertThat(latch.await(30000, TimeUnit.MILLISECONDS), is(true));
            assertEquals(0, latch.getCount());
            assertThat(faulty.get(), is(false));
            assertThat(expected.get(), is(true));

            assertThat(recordingAppender.getMessages().stream().anyMatch(m -> m.contains("Pausing response writing as writeBufferHighWaterMark exceeded on")), is(true));
        } catch (Exception ex) {
            fail("Shouldn't have tossed an exception");
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldReturnInvalidRequestArgsWhenGremlinArgIsNotSupplied() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL).create();
            final ResponseMessage result = client.submit(request).get(0);
            assertThat(result.getStatus().getCode(), is(not(ResponseStatusCode.PARTIAL_CONTENT)));
            assertEquals(result.getStatus().getCode(), ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS);
        }
    }

    @Test
    public void shouldReturnInvalidRequestArgsWhenInvalidReservedBindingKeyIsUsed() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()) {
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
            assertThat(pass.get(), is(true));
        }

        try (SimpleClient client = TestClientFactory.createWebSocketClient()) {
            final Map<String, Object> bindings = new HashMap<>();
            bindings.put("id", "123");
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
        try (SimpleClient client = TestClientFactory.createWebSocketClient()) {
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
            assertThat(pass.get(), is(true));
        }
    }

    @Test
    public void shouldReturnInvalidRequestArgsWhenBindingCountExceedsAllowable() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()) {
            final Map<Object, Object> bindings = new HashMap<>();
            bindings.put("x", 123);
            bindings.put("y", 123);
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "x+y")
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
            assertThat(pass.get(), is(true));
        }

        try (SimpleClient client = TestClientFactory.createWebSocketClient()) {
            final Map<Object, Object> bindings = new HashMap<>();
            bindings.put("x", 123);
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "x+123")
                    .addArg(Tokens.ARGS_BINDINGS, bindings).create();
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicBoolean pass = new AtomicBoolean(false);
            client.submit(request, result -> {
                if (result.getStatus().getCode() != ResponseStatusCode.PARTIAL_CONTENT) {
                    pass.set(ResponseStatusCode.SUCCESS == result.getStatus().getCode() && (((int) ((List) result.getResult().getData()).get(0) == 246)));
                    latch.countDown();
                }
            });

            if (!latch.await(3000, TimeUnit.MILLISECONDS))
                fail("Request should have returned error, but instead timed out");
            assertThat(pass.get(), is(true));
        }
    }

    @Test
    public void shouldReturnInvalidRequestArgsWhenInvalidNullBindingKeyIsUsed() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()) {
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
            assertThat(pass.get(), is(true));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldBatchResultsByTwos() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "[0,1,2,3,4,5,6,7,8,9]").create();

            final List<ResponseMessage> msgs = client.submit(request);
            assertEquals(5, client.submit(request).size());
            assertEquals(0, ((List<Integer>) msgs.get(0).getResult().getData()).get(0).intValue());
            assertEquals(1, ((List<Integer>) msgs.get(0).getResult().getData()).get(1).intValue());
            assertEquals(2, ((List<Integer>) msgs.get(1).getResult().getData()).get(0).intValue());
            assertEquals(3, ((List<Integer>) msgs.get(1).getResult().getData()).get(1).intValue());
            assertEquals(4, ((List<Integer>) msgs.get(2).getResult().getData()).get(0).intValue());
            assertEquals(5, ((List<Integer>) msgs.get(2).getResult().getData()).get(1).intValue());
            assertEquals(6, ((List<Integer>) msgs.get(3).getResult().getData()).get(0).intValue());
            assertEquals(7, ((List<Integer>) msgs.get(3).getResult().getData()).get(1).intValue());
            assertEquals(8, ((List<Integer>) msgs.get(4).getResult().getData()).get(0).intValue());
            assertEquals(9, ((List<Integer>) msgs.get(4).getResult().getData()).get(1).intValue());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldBatchResultsByOnesByOverridingFromClientSide() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "[0,1,2,3,4,5,6,7,8,9]")
                    .addArg(Tokens.ARGS_BATCH_SIZE, 1).create();

            final List<ResponseMessage> msgs = client.submit(request);
            assertEquals(10, msgs.size());
            IntStream.rangeClosed(0, 9).forEach(i -> assertEquals(i, ((List<Integer>) msgs.get(i).getResult().getData()).get(0).intValue()));
        }
    }

    @Test
    public void shouldNotThrowNoSuchElementException() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()){
            // this should return "nothing" - there should be no exception
            final List<ResponseMessage> responses = client.submit("g.V().has('name','kadfjaldjfla')");
            assertNull(responses.get(0).getResult().getData());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReceiveFailureTimeOutOnScriptEval() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()){
            final List<ResponseMessage> responses = client.submit("Thread.sleep(3000);'some-stuff-that-should not return'");
            assertThat(responses.get(0).getStatus().getMessage(), startsWith("Evaluation exceeded the configured 'evaluationTimeout' threshold of 1000 ms"));

            // validate that we can still send messages to the server
            assertEquals(2, ((List<Integer>) client.submit("1+1").get(0).getResult().getData()).get(0).intValue());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReceiveFailureTimeOutOnEvalUsingOverride() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()) {
            final RequestMessage msg = RequestMessage.build("eval")
                    .addArg(Tokens.ARGS_EVAL_TIMEOUT, 100L)
                    .addArg(Tokens.ARGS_GREMLIN, "Thread.sleep(3000);'some-stuff-that-should not return'")
                    .create();
            final List<ResponseMessage> responses = client.submit(msg);
            assertThat(responses.get(0).getStatus().getMessage(), startsWith("Evaluation exceeded the configured 'evaluationTimeout' threshold of 100 ms"));

            // validate that we can still send messages to the server
            assertEquals(2, ((List<Integer>) client.submit("1+1").get(0).getResult().getData()).get(0).intValue());
        }
    }

    @Test
    public void shouldReceiveFailureTimeOutOnScriptEvalOfOutOfControlLoop() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()){
            // timeout configured for 1 second so the timed interrupt should trigger prior to the
            // evaluationTimeout which is at 30 seconds by default
            final List<ResponseMessage> responses = client.submit("while(true){}");
            assertThat(responses.get(0).getStatus().getMessage(), startsWith("Timeout during script evaluation triggered by TimedInterruptCustomizerProvider"));

            // validate that we can still send messages to the server
            assertEquals(2, ((List<Integer>) client.submit("1+1").get(0).getResult().getData()).get(0).intValue());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldLoadInitScript() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()){
            assertEquals(2, ((List<Integer>) client.submit("addItUp(1,1)").get(0).getResult().getData()).get(0).intValue());
        }
    }

    @Test
    public void shouldGarbageCollectPhantomButNotHard() throws Exception {
        final Cluster cluster = TestClientFactory.open();
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
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V3D0).create();
        final Client client = cluster.connect();

        try {
            client.submit("def class C { def C getC(){return this}}; new C()").all().join();
            fail("Should throw an exception.");
        } catch (RuntimeException re) {
            final Throwable root = ExceptionUtils.getRootCause(re);
            assertThat(root.getMessage(), CoreMatchers.startsWith("Error during serialization: Direct self-reference leading to cycle (through reference chain:"));

            // validate that we can still send messages to the server
            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldReceiveFailureOnBadGryoSerialization() throws Exception {
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRYO_V1D0).create();
        final Client client = cluster.connect();

        try {
            client.submit("java.awt.Color.RED").all().join();
            fail("Should throw an exception.");
        } catch (RuntimeException re) {
            final Throwable root = ExceptionUtils.getRootCause(re);
            assertThat(root.getMessage(), CoreMatchers.startsWith("Error during serialization: Class is not registered: java.awt.Color"));

            // validate that we can still send messages to the server
            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    @Test
    public void shouldBlockRequestWhenTooBig() throws Exception {
        final Cluster cluster = TestClientFactory.open();
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

            // went with two possible error messages here as i think that there is some either non-deterministic
            // behavior around the error message or it's environmentally dependent (e.g. different jdk, versions, etc)
            assertThat(root.getMessage(), Matchers.anyOf(is("Connection to server is no longer active"), is("Connection reset by peer")));

            // validate that we can still send messages to the server
            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldFailOnDeadHost() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        // ensure that connection to server is good
        assertEquals(2, client.submit("1+1").all().join().get(0).getInt());

        // kill the server which will make the client mark the host as unavailable
        this.stopServer();

        try {
            // try to re-issue a request now that the server is down
            client.submit("g").all().join();
            fail("Should throw an exception.");
        } catch (RuntimeException re) {
            // Client would have no active connections to the host, hence it would encounter a timeout
            // trying to find an alive connection to the host.
            assertThat(re.getCause().getCause() instanceof TimeoutException, is(true));

            //
            // should recover when the server comes back
            //

            // restart server
            this.startServer();

            // try a bunch of times to reconnect. on slower systems this may simply take longer...looking at you travis
            for (int ix = 1; ix < 11; ix++) {
                // the retry interval is 1 second, wait a bit longer
                TimeUnit.SECONDS.sleep(5);

                try {
                    final List<Result> results = client.submit("1+1").all().join();
                    assertEquals(1, results.size());
                    assertEquals(2, results.get(0).getInt());
                } catch (Exception ex) {
                    if (ix == 10)
                        fail("Should have eventually succeeded");
                }
            }
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldNotHavePartialContentWithOneResult() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "10").create();
            final List<ResponseMessage> responses = client.submit(request);
            assertEquals(1, responses.size());
            assertEquals(ResponseStatusCode.SUCCESS, responses.get(0).getStatus().getCode());
        }
    }

    @Test
    public void shouldHavePartialContentWithLongResultsCollection() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "new String[100]").create();
            final List<ResponseMessage> responses = client.submit(request);
            assertThat(responses.size(), Matchers.greaterThan(1));
            for (Iterator<ResponseMessage> it = responses.iterator(); it.hasNext(); ) {
                final ResponseMessage msg = it.next();
                final ResponseStatusCode expected = it.hasNext() ? ResponseStatusCode.PARTIAL_CONTENT : ResponseStatusCode.SUCCESS;
                assertEquals(expected, msg.getStatus().getCode());
            }
        }
    }

    @Test
    public void shouldFailWithBadScriptEval() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "new String().doNothingAtAllBecauseThis is a syntax error").create();
            final List<ResponseMessage> responses = client.submit(request);
            assertEquals(ResponseStatusCode.SERVER_ERROR_EVALUATION, responses.get(0).getStatus().getCode());
            assertEquals(1, responses.size());
        }
    }

    @Test
    public void shouldSupportLambdasUsingWithRemote() throws Exception {
        final GraphTraversalSource g = traversal().withRemote(conf);
        g.addV("person").property("age", 20).iterate();
        g.addV("person").property("age", 10).iterate();
        assertEquals(50L, g.V().hasLabel("person").map(Lambda.function("it.get().value('age') + 10")).sum().next());
    }

    @Test
    public void shouldDoNonBlockingPromiseWithRemote() throws Exception {
        final GraphTraversalSource g = traversal().withRemote(conf);
        g.addV("person").property("age", 20).promise(Traversal::iterate).join();
        g.addV("person").property("age", 10).promise(Traversal::iterate).join();
        assertEquals(50L, g.V().hasLabel("person").map(Lambda.function("it.get().value('age') + 10")).sum().promise(t -> t.next()).join());
        g.addV("person").property("age", 20).promise(Traversal::iterate).join();

        final Traversal<Vertex,Integer> traversal = g.V().hasLabel("person").has("age", 20).values("age");
        int age = traversal.promise(t -> t.next(1).get(0)).join();
        assertEquals(20, age);
        assertEquals(20, (int)traversal.next());
        assertThat(traversal.hasNext(), is(false));

        final Traversal traversalCloned = g.V().hasLabel("person").has("age", 20).values("age");
        assertEquals(20, traversalCloned.next());
        assertEquals(20, traversalCloned.promise(t -> ((Traversal) t).next(1).get(0)).join());
        assertThat(traversalCloned.promise(t -> ((Traversal) t).hasNext()).join(), is(false));

        assertEquals(3, g.V().promise(Traversal::toList).join().size());
    }

    @Test
    public void shouldProvideBetterExceptionForMethodCodeTooLarge() {
        final int numberOfParameters = 4000;
        final Map<String,Object> b = new HashMap<>();

        // generate a script with a ton of bindings usage to generate a "code too large" exception
        String script = "x = 0";
        for (int ix = 0; ix < numberOfParameters; ix++) {
            if (ix > 0 && ix % 100 == 0) {
                script = script + ";" + System.lineSeparator() + "x = x";
            }
            script = script + " + x" + ix;
            b.put("x" + ix, ix);
        }

        final Cluster cluster = TestClientFactory.build().maxContentLength(4096000).create();
        final Client client = cluster.connect();

        try {
            client.submit(script, b).all().get();
            fail("Should have tanked out because of number of parameters used and size of the compile script");
        } catch (Exception ex) {
            assertThat(ex.getMessage(), containsString("The Gremlin statement that was submitted exceeds the maximum compilation size allowed by the JVM"));
        }
    }

    @Test
    public void shouldGenerateTemporaryErrorResponseStatusCode() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            client.submit("g.addV('person').sideEffect{throw new org.apache.tinkerpop.gremlin.server.util.DefaultTemporaryException('try again!')}").all().get();
            fail("Should have tanked since we threw an exception out manually");
        } catch (Exception ex) {
            final Throwable t = ex.getCause();
            assertThat(t, instanceOf(ResponseException.class));
            assertEquals("try again!", t.getMessage());
            assertEquals(ResponseStatusCode.SERVER_ERROR_TEMPORARY, ((ResponseException) t).getResponseStatusCode());
        }
    }
}
