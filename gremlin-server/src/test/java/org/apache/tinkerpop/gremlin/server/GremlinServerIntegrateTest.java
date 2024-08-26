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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import nl.altindag.log.LogCaptor;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.server.channel.HttpTestChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.TestChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.UnifiedChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.UnifiedTestChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.WebSocketTestChannelizer;
import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpTestChannelizer;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.apache.tinkerpop.gremlin.driver.simple.SimpleClient;
import org.apache.tinkerpop.gremlin.driver.UserAgent;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyCompilerGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.SimpleSandboxExtension;
import org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin;
import org.apache.tinkerpop.gremlin.server.handler.OpSelectorHandler;
import org.apache.tinkerpop.gremlin.server.handler.UnifiedHandler;
import org.apache.tinkerpop.gremlin.structure.RemoteGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.server.op.AbstractEvalOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;
import org.apache.tinkerpop.gremlin.server.handler.WsUserAgentHandler;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.tinkerpop.gremlin.util.Tokens.ARGS_EVAL_TIMEOUT;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyCompilerGremlinPlugin.Compilation.COMPILE_STATIC;
import static org.apache.tinkerpop.gremlin.process.remote.RemoteConnection.GREMLIN_REMOTE;
import static org.apache.tinkerpop.gremlin.process.remote.RemoteConnection.GREMLIN_REMOTE_CONNECTION_CLASS;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

/**
 * Integration tests for server-side settings and processing.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServerIntegrateTest extends AbstractGremlinServerIntegrationTest {

    private Level previousLogLevel;
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(GremlinServerIntegrateTest.class);

    private final Supplier<Graph> graphGetter = () -> server.getServerGremlinExecutor().getGraphManager().getGraph("graph");
    private final Configuration conf = new BaseConfiguration() {{
        setProperty(Graph.GRAPH, RemoteGraph.class.getName());
        setProperty(GREMLIN_REMOTE_CONNECTION_CLASS, DriverRemoteConnection.class.getName());
        setProperty(DriverRemoteConnection.GREMLIN_REMOTE_DRIVER_SOURCENAME, "g");
        setProperty(GREMLIN_REMOTE + "attachment", graphGetter);
        setProperty("clusterConfiguration.port", TestClientFactory.PORT);
        setProperty("clusterConfiguration.hosts", "localhost");
    }};
    private static final int POOL_SIZE_FOR_TIMEOUT_TESTS = 1;

    private static LogCaptor logCaptor;

    @BeforeClass
    public static void setupLogCaptor() {
        logCaptor = LogCaptor.forRoot();
    }

    @AfterClass
    public static void tearDownAfterClass() {
        logCaptor.close();
    }

    @Before
    public void setupForEachTest() {

        if (name.getMethodName().equals("shouldPingChannelIfClientDies") ||
                name.getMethodName().equals("shouldCloseChannelIfClientDoesntRespond") ||
                name.getMethodName().equals("shouldCaptureUserAgentFromClient")) {
            final Logger opSelectorHandlerLogger = (Logger) LoggerFactory.getLogger(OpSelectorHandler.class);
            final Logger unifiedHandlerLogger = (Logger) LoggerFactory.getLogger(UnifiedHandler.class);
            final Logger wsUserAgentHandlerLogger = (Logger) LoggerFactory.getLogger(WsUserAgentHandler.class);
            previousLogLevel = opSelectorHandlerLogger.getLevel();
            opSelectorHandlerLogger.setLevel(Level.INFO);
            unifiedHandlerLogger.setLevel(Level.INFO);
            wsUserAgentHandlerLogger.setLevel(Level.DEBUG);
        }

        logCaptor.clearLogs();
    }

    @After
    public void teardownForEachTest() {
        if (name.getMethodName().equals("shouldPingChannelIfClientDies") ||
                name.getMethodName().equals("shouldCloseChannelIfClientDoesntRespond") ||
                name.getMethodName().equals("shouldCaptureUserAgentFromClient")) {
            final Logger opSelectorHandlerLogger = (Logger) LoggerFactory.getLogger(OpSelectorHandler.class);
            opSelectorHandlerLogger.setLevel(previousLogLevel);
            final Logger unifiedHandlerLogger = (Logger) LoggerFactory.getLogger(UnifiedHandler.class);
            unifiedHandlerLogger.setLevel(previousLogLevel);
            final Logger wsUserAgentHandlerLogger = (Logger) LoggerFactory.getLogger(WsUserAgentHandler.class);
            wsUserAgentHandlerLogger.setLevel(previousLogLevel);
        }
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

                // OpProcessor setting
                final Settings.ProcessorSettings processorSettingsBig = new Settings.ProcessorSettings();
                processorSettingsBig.className = StandardOpProcessor.class.getName();
                processorSettingsBig.config = new HashMap<String,Object>() {{
                    put(AbstractEvalOpProcessor.CONFIG_MAX_PARAMETERS, Integer.MAX_VALUE);
                }};
                settings.processors.clear();
                settings.processors.add(processorSettingsBig);

                // Unified setting
                settings.maxParameters = Integer.MAX_VALUE;
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
            case "shouldBatchResultsByTwosToDriver":
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
                // OpProcessor settings
                final Settings.ProcessorSettings processorSettingsSmall = new Settings.ProcessorSettings();
                processorSettingsSmall.className = StandardOpProcessor.class.getName();
                processorSettingsSmall.config = new HashMap<String,Object>() {{
                    put(AbstractEvalOpProcessor.CONFIG_MAX_PARAMETERS, 1);
                }};
                settings.processors.clear();
                settings.processors.add(processorSettingsSmall);

                // Unified settings
                settings.maxParameters = 1;
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
            case "shouldBlowTheWorkQueueSize":
                settings.gremlinPool = 1;
                settings.maxWorkQueueSize = 1;
                break;
            case "shouldRespondToTimeoutCancelledWsRequest":
            case "shouldRespondToTimeoutCancelledSessionRequest":
                useTinkerTransactionGraph(settings);
                settings.evaluationTimeout = 5000;
                settings.gremlinPool = POOL_SIZE_FOR_TIMEOUT_TESTS;
                settings.channelizer = WebSocketChannelizer.class.getName();
                break;
            case "shouldRespondToTimeoutCancelledSingleTaskUnifiedRequest":
                settings.evaluationTimeout = 5000;
                settings.gremlinPool = POOL_SIZE_FOR_TIMEOUT_TESTS;
                settings.channelizer = UnifiedChannelizer.class.getName();
                break;
            case "shouldRespondToTimeoutCancelledMultiTaskUnifiedRequest":
                useTinkerTransactionGraph(settings);
                settings.evaluationTimeout = 30000;
                settings.sessionLifetimeTimeout = 5000; // This needs to be shorter because of the delay in scheduling session task.
                settings.gremlinPool = POOL_SIZE_FOR_TIMEOUT_TESTS;
                settings.channelizer = UnifiedChannelizer.class.getName();
                break;
            case "shouldStoreUserAgentInContextWebSocket":
                settings.channelizer = WebSocketTestChannelizer.class.getName();
                break;
            case "shouldStoreUserAgentInContextHttp":
                settings.channelizer = HttpTestChannelizer.class.getName();
                break;
            case "shouldStoreUserAgentInContextWsAndHttp":
                settings.channelizer = WsAndHttpTestChannelizer.class.getName();
                break;
            case "shouldStoreUserAgentInContextUnified":
                settings.channelizer = UnifiedTestChannelizer.class.getName();
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
    public void shouldCaptureUserAgentFromClient() {
        final Cluster cluster = TestClientFactory.build().enableUserAgentOnConnect(true).create();
        final Client client = cluster.connect();
        client.submit("test");

        assertThat(logCaptor.getLogs().stream().anyMatch(m -> m.matches(
                ".*New Connection on channel .* with user agent.*$")), is(true));

        cluster.close();
    }

    @Test
    public void shouldStoreUserAgentInContextWebSocket() throws InterruptedException {
        shouldStoreUserAgentInContext();
    }

    @Test
    public void shouldStoreUserAgentInContextHttp() throws InterruptedException {
        shouldStoreUserAgentInHttpContext();
    }

    @Test
    public void shouldStoreUserAgentInContextWsAndHttp() throws InterruptedException {
        shouldStoreUserAgentInContext();
        shouldStoreUserAgentInHttpContext();
    }

    @Test
    public void shouldStoreUserAgentInContextUnified() throws InterruptedException {
        shouldStoreUserAgentInContext();
        shouldStoreUserAgentInHttpContext();
    }

    private void shouldStoreUserAgentInContext() {
        if(server.getChannelizer() instanceof TestChannelizer) {
            TestChannelizer channelizer = (TestChannelizer) server.getChannelizer();
            channelizer.resetChannelHandlerContext();
            assertNull(getUserAgentIfAvailable());
            final Cluster cluster = TestClientFactory.build().enableUserAgentOnConnect(true).create();
            final Client client = cluster.connect();

            client.submit("test");
            assertEquals(UserAgent.USER_AGENT, getUserAgentIfAvailable());
            client.submit("test");
            assertEquals(UserAgent.USER_AGENT, getUserAgentIfAvailable());
            client.close();
            cluster.close();
        }
    }

    private void shouldStoreUserAgentInHttpContext() throws InterruptedException {
        if(server.getChannelizer() instanceof TestChannelizer) {
            TestChannelizer channelizer = (TestChannelizer) server.getChannelizer();
            channelizer.resetChannelHandlerContext();
            assertNull(getUserAgentIfAvailable());
            final Cluster cluster = TestClientFactory.build()
                    .channelizer(org.apache.tinkerpop.gremlin.driver.Channelizer.HttpChannelizer.class)
                    .enableUserAgentOnConnect(true)
                    .create();
            final Client client = cluster.connect();

            client.submit("g.V()");
            waitForUserAgentAvailability();
            assertEquals(UserAgent.USER_AGENT, getUserAgentIfAvailable());
            client.submit("g.V()");
            waitForUserAgentAvailability();
            assertEquals(UserAgent.USER_AGENT, getUserAgentIfAvailable());
            client.close();
            cluster.close();
        }
    }

    private String getUserAgentIfAvailable() {
        final AttributeKey<String> userAgentAttrKey = AttributeKey.valueOf(UserAgent.USER_AGENT_HEADER_NAME);
        if(!(server.getChannelizer() instanceof TestChannelizer)) {
            return null;
        }
        final TestChannelizer channelizer = (TestChannelizer) server.getChannelizer();
        final ChannelHandlerContext ctx = channelizer.getMostRecentChannelHandlerContext();
        if(ctx == null || !ctx.channel().hasAttr(userAgentAttrKey)) {
            return null;
        }
        return channelizer.getMostRecentChannelHandlerContext().channel().attr(userAgentAttrKey).get();
    }

    private void waitForUserAgentAvailability() throws InterruptedException {
        final int maxWait = 2000;
        final int waitInterval = 50;
        for(int i = 0; i < maxWait; i += waitInterval) {
            if (getUserAgentIfAvailable() != null) {
                return;
            }
            java.lang.Thread.sleep(waitInterval);
        }
    }

    @Test
    public void shouldBlowTheWorkQueueSize() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();

        // maxWorkQueueSize=1 && gremlinPool=1
        // we should be able to do one request at a time serially
        assertEquals("test1", client.submit("'test1'").all().get().get(0).getString());
        assertEquals("test2", client.submit("'test2'").all().get().get(0).getString());
        assertEquals("test3", client.submit("'test3'").all().get().get(0).getString());

        final AtomicBoolean errorTriggered = new AtomicBoolean();
        final ResultSet r1 = client.submitAsync("Thread.sleep(1000);'test4'").get();

        final List<CompletableFuture<List<Result>>> blockers = new ArrayList<>();
        for (int ix = 0; ix < 512 && !errorTriggered.get(); ix++) {
            blockers.add(client.submit("'test'").all().exceptionally(t -> {
                final ResponseException re = (ResponseException) t.getCause();
                errorTriggered.compareAndSet(false, ResponseStatusCode.TOO_MANY_REQUESTS == re.getResponseStatusCode());
                return null;
            }));
        }

        assertThat(errorTriggered.get(), is(true));

        // wait for the blockage to clear for sure
        assertEquals("test4", r1.all().get().get(0).getString());
        blockers.forEach(CompletableFuture::join);

        // should be accepting test6 now
        assertEquals("test6", client.submit("'test6'").all().get().get(0).getString());

        cluster.close();
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

        g.close();
    }

    @Test
    public void shouldCloseChannelIfClientDoesntRespond() throws Exception {
        final SimpleClient client = TestClientFactory.createWebSocketClient();
        client.submit("1+1");

        // since we do nothing for 2 seconds and the time limit for timeout on the server is 1 second, the server
        // will autoclose the channel
        Thread.sleep(2000);

        assertThat(logCaptor.getLogs().stream().anyMatch(m -> m.matches(
                ".*Closing channel - client is disconnected after idle period of .*$")), is(true));

        client.close();
    }

    @Test
    public void shouldPingChannelIfClientDies() throws Exception {
        final Cluster cluster = TestClientFactory.build().maxConnectionPoolSize(1).minConnectionPoolSize(1).keepAliveInterval(0).create();
        final Client client = cluster.connect();
        client.submit("1+1").all().get();

        // since we do nothing for 3 seconds and the time limit for ping is 1 second we should get *about* 3 pings -
        // i don't think the assertion needs to be too accurate. just need to make sure there's a ping message out
        // there record
        Thread.sleep(3000);

        cluster.close();

        // stop the server to be sure that logs flush
        stopServer();

        assertThat(logCaptor.getLogs().stream().anyMatch(m -> m.matches(
                ".*Checking channel - sending ping to client after idle period of .*$")), is(true));
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

        g.close();
    }

    @Test
    public void shouldTimeOutRemoteTraversalWithPerRequestOption() throws Exception {
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

        g.close();
    }

    @Test
    public void shouldProduceProperExceptionOnTimeout() throws Exception {
        // this test will not work quite right on UnifiedChannelizer
        assumeThat("Must use OpProcessor", isUsingUnifiedChannelizer(), is(false));

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

            assertThat(logCaptor.getLogs().stream().anyMatch(m -> m.contains(
                    "Pausing response writing as writeBufferHighWaterMark exceeded on")), is(true));
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
        // this test will not work quite right on UnifiedChannelizer, but seems to only be a problem in Travis
        assumeThat("Must use OpProcessor", isUsingUnifiedChannelizer(), is(false));

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

//    @Test
//    public void shouldBatchResultsByTwosWithDriver() throws Exception {
//        final Cluster cluster = TestClientFactory.build().create();
//        final Client client = cluster.connect();
//
//        try {
//            final List<Result> results = client.submit("[0,1,2,3,4,5,6,7,8,9]").all().join();
//            for (int ix = 0; ix < results.size(); ix++) {
//                assertEquals(ix, results.get(ix).getInt());
//            }
//        } finally {
//            cluster.close();
//        }
//    }

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
            assertThat(responses.get(0).getStatus().getMessage(), allOf(startsWith("Evaluation exceeded"), containsString("1000 ms")));

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
            assertThat(responses.get(0).getStatus().getMessage(), allOf(startsWith("Evaluation exceeded"), containsString("100 ms")));

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
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V3).create();
        final Client client = cluster.connect();

        try {
            client.submit("class C { def C getC(){return this}}; new C()").all().join();
            fail("Should throw an exception.");
        } catch (RuntimeException re) {
            final Throwable root = ExceptionHelper.getRootCause(re);
            assertThat(root.getMessage(), CoreMatchers.startsWith("Error during serialization: Direct self-reference leading to cycle (through reference chain:"));

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
            final Throwable root = ExceptionHelper.getRootCause(re);

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
        final Cluster cluster = TestClientFactory.build().reconnectInterval(1000).create();
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
            assertThat(re.getCause(), instanceOf(TimeoutException.class));

            //
            // should recover when the server comes back
            //

            // restart server
            this.startServer();

            // try a bunch of times to reconnect. on slower systems this may simply take longer...looking at you travis
            for (int ix = 1; ix < 11; ix++) {
                // the retry interval is 1 second, wait a bit longer
                TimeUnit.SECONDS.sleep(5);

                logger.warn("Waiting for reconnect on restarted host: {}", ix);

                try {
                    // just need to succeed at reconnect one time
                    final List<Result> results = client.submit("1+1").all().join();
                    assertEquals(1, results.size());
                    assertEquals(2, results.get(0).getInt());
                    break;
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
        g.close();
    }

    @Test
    public void shouldDoNonBlockingPromiseWithRemote() throws Exception {
        final GraphTraversalSource g = traversal().withRemote(conf);
        g.addV("person").property("age", 20).promise(Traversal::iterate).join();
        g.addV("person").property("age", 10).promise(Traversal::iterate).join();
        assertEquals(50L, g.V().hasLabel("person").map(Lambda.function("it.get().value('age') + 10")).sum().promise(t -> t.next()).join());
        g.addV("person").property("age", 20).promise(Traversal::iterate).join();

        final Traversal<Vertex,Integer> traversal = g.V().hasLabel("person").has("age", 20).values("age");
        final int age = traversal.promise(t -> t.next(1).get(0)).join();
        assertEquals(20, age);
        assertEquals(20, (int)traversal.next());
        assertThat(traversal.hasNext(), is(false));

        final Traversal traversalCloned = g.V().hasLabel("person").has("age", 20).values("age");
        assertEquals(20, traversalCloned.next());
        assertEquals(20, traversalCloned.promise(t -> ((Traversal) t).next(1).get(0)).join());
        assertThat(traversalCloned.promise(t -> ((Traversal) t).hasNext()).join(), is(false));

        assertEquals(3, g.V().promise(Traversal::toList).join().size());

        g.close();
    }

    @Test
    public void shouldProvideBetterExceptionForMethodCodeTooLarge() {
        final int numberOfParameters = 6000;
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
        } finally {
            cluster.close();
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
        } finally {
            cluster.close();
        }
    }

    /**
     * Reproducer for TINKERPOP-2769 with request sent to WebSocketChannelizer.
     */
    @Test(timeout = 180000) // Add timeout in case the test hangs.
    public void shouldRespondToTimeoutCancelledWsRequest() throws Exception {
        final GraphTraversalSource g = traversal().withRemote(conf);
        runTimeoutTest(g);
        g.close();
    }

    /**
     * Reproducer for TINKERPOP-2769 with request having a Session ID sent to WebSocketChannelizer.
     */
    @Test(timeout = 180000) // Add timeout in case the test hangs.
    public void shouldRespondToTimeoutCancelledSessionRequest() throws Exception {
        // Don't test with UnifiedChannelizer since we only want to test the case where a task is cancelled before
        // running which is handled by shouldRespondToTimeoutCancelledMultiTaskUnifiedRequest.
        assumeThat("Must use OpProcessor", isUsingUnifiedChannelizer(), is(false));

        final Cluster cluster = TestClientFactory.build().create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
        final GraphTraversalSource gtx = g.tx().begin();

        runTimeoutTest(gtx);

        gtx.tx().commit();
        cluster.close();
    }

    /**
     * Reproducer for TINKERPOP-2769 with request sent to UnifiedChannelizer.
     */
    @Test(timeout = 180000) // Add timeout in case the test hangs.
    public void shouldRespondToTimeoutCancelledSingleTaskUnifiedRequest() throws Exception {
        final GraphTraversalSource g = traversal().withRemote(conf);
        runTimeoutTest(g);
        g.close();
    }

    /**
     * Reproducer for TINKERPOP-2769 with request having a Session ID sent to UnifiedChannelizer.
     */
    @Test(timeout = 180000) // Add timeout in case the test hangs.
    public void shouldRespondToTimeoutCancelledMultiTaskUnifiedRequest() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
        final GraphTraversalSource gtx = g.tx().begin();

        g.addV("person").as("p").addE("self").to("p").iterate();

        // Number of threads/tasks must be larger than the size of the gremlinPool set in overrideSettings().
        final ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(2);
        // Use a request without session to fill the queue since we want to test cancel on first session request.
        final Future<?> firstResult = threadPool.submit(() -> g.V().repeat(__.out()).until(__.outE().count().is(0)).iterate());
        // Delay this request slightly as we want to test that the MultiTaskSession is properly returning an error when it is cancelled.
        final Future<?> secondResult = threadPool.schedule(() -> gtx.V().repeat(__.out()).until(__.outE().count().is(0)).iterate(), 5000, TimeUnit.MILLISECONDS);

        try {
            firstResult.get();
            fail("This traversal should have timed out");
        } catch (Exception ex) {
            final Throwable t = ex.getCause().getCause(); // Get the nested ResponseException.
            assertThat(t, instanceOf(ResponseException.class));
            assertEquals(ResponseStatusCode.SERVER_ERROR_TIMEOUT, ((ResponseException) t).getResponseStatusCode());
        }

        try {
            secondResult.get();
            fail("This traversal should have timed out");
        } catch (Exception ex) {
            final Throwable t = ex.getCause().getCause(); // Get the nested ResponseException.
            assertThat(t, instanceOf(ResponseException.class));
            assertEquals(ResponseStatusCode.SERVER_ERROR_TIMEOUT, ((ResponseException) t).getResponseStatusCode());
        }

        threadPool.shutdown();
        gtx.tx().rollback();
        cluster.close();
    }

    private void runTimeoutTest(GraphTraversalSource g) throws Exception {
        // make a graph with a cycle in it to force a long run traversal
        g.addV("person").as("p").addE("self").to("p").iterate();

        // Number of threads/tasks must be larger than the size of the gremlinPool set in overrideSettings().
        final int numTasksNeededToOverloadPool = POOL_SIZE_FOR_TIMEOUT_TESTS + 2;
        final ExecutorService threadPool = Executors.newFixedThreadPool(numTasksNeededToOverloadPool);
        // test "unending" traversals
        final List<Future<?>> results = new ArrayList<>();
        for (int i = 0; i < numTasksNeededToOverloadPool; i++) {
            results.add(threadPool.submit(() -> g.V().repeat(__.out()).until(__.outE().count().is(0)).iterate()));
        }

        for (final Future<?> result : results) {
            try {
                result.get();
                fail("This traversal should have timed out");
            } catch (Exception ex) {
                final Throwable t = ex.getCause().getCause(); // Get the nested ResponseException.
                assertThat(t, instanceOf(ResponseException.class));
                assertEquals(ResponseStatusCode.SERVER_ERROR_TIMEOUT, ((ResponseException) t).getResponseStatusCode());
            }
        }

        threadPool.shutdown();
    }

    /**
     * Reproducer for TINKERPOP-2765 when run using the UnifiedChannelizer.
     */
    @Test
    public void shouldHandleMultipleLambdaTranslationsInParallel() throws Exception {
        final GraphTraversalSource g = traversal().withRemote(conf);

        final CompletableFuture<Traversal<Object, Object>> firstRes = g.with("evaluationTimeout", 90000L).inject(1).sideEffect(Lambda.consumer("Thread.sleep(100)")).promise(Traversal::iterate);
        final CompletableFuture<Traversal<Object, Object>> secondRes = g.with("evaluationTimeout", 90000L).inject(1).sideEffect(Lambda.consumer("Thread.sleep(100)")).promise(Traversal::iterate);
        final CompletableFuture<Traversal<Object, Object>> thirdRes = g.with("evaluationTimeout", 90000L).inject(1).sideEffect(Lambda.consumer("Thread.sleep(100)")).promise(Traversal::iterate);

        try {
            firstRes.get();
            secondRes.get();
            thirdRes.get();
        } catch (Exception ce) {
            fail("An exception was not expected because the traversals are all valid.");
        } finally {
            g.close();
        }
    }

    @Test
    public void shouldGenerateFailureErrorResponseStatusCode() throws Exception {
        final Cluster cluster = TestClientFactory.build().create();
        final Client client = cluster.connect();

        try {
            client.submit("g.inject(0).fail('make it stop')").all().get();
            fail("Should have tanked since we used fail() step");
        } catch (Exception ex) {
            final Throwable t = ex.getCause();
            assertThat(t, instanceOf(ResponseException.class));
            assertEquals("make it stop", t.getMessage());
            assertEquals(ResponseStatusCode.SERVER_ERROR_FAIL_STEP, ((ResponseException) t).getResponseStatusCode());
        }
    }

    @Test
    public void shouldReturnEmptyPropertiesWithMaterializeProperties() {
        final Cluster cluster = TestClientFactory.build().create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        final Vertex v1 = g.addV("person").property("name", "marko").next();
        final Vertex r1 = g.V().next();
        assertEquals(v1.properties().next(), r1.properties().next());
        final Vertex r1_tokens = g.with("materializeProperties", "tokens").V().next();
        assertFalse(r1_tokens.properties().hasNext());

        final VertexProperty vp1 = (VertexProperty) g.with("materializeProperties", "tokens").V().properties().next();
        assertFalse(vp1.properties().hasNext());

        final Vertex v2 = g.addV("person").property("name", "stephen").next();
        g.V(v1).addE("knows").to(v2).property("weight", 0.75).iterate();
        final Edge r2 = g.E().next();
        assertEquals(r2.properties().next(), r2.properties().next());
        final Edge r2_tokens = g.with("materializeProperties", "tokens").E().next();
        assertFalse(r2_tokens.properties().hasNext());
    }
}
