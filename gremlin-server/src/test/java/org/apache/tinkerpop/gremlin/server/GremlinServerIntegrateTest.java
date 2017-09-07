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

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.exception.ExceptionUtils;
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
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteTraversalSideEffects;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.driver.simple.SimpleClient;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.CompileStaticCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.ConfigurationCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.InterpreterModeCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.SimpleSandboxExtension;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.TimedInterruptCustomizerProvider;
import org.apache.tinkerpop.gremlin.process.remote.RemoteGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.server.op.AbstractEvalOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.standard.StandardOpProcessor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.Log4jRecordingAppender;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.tinkerpop.gremlin.process.traversal.TraversalSource.GREMLIN_REMOTE_CONNECTION_CLASS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.StringEndsWith.endsWith;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

/**
 * Integration tests for server-side settings and processing.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServerIntegrateTest extends AbstractGremlinServerIntegrationTest {

    private static final String SERVER_KEY = "src/test/resources/server.key.pk8";
    private static final String SERVER_CRT = "src/test/resources/server.crt";
    private static final String KEY_PASS = "changeit";
    private static final String CLIENT_KEY = "src/test/resources/client.key.pk8";
    private static final String CLIENT_CRT = "src/test/resources/client.crt";

    private Log4jRecordingAppender recordingAppender = null;
    private final Supplier<Graph> graphGetter = () -> server.getServerGremlinExecutor().getGraphManager().getGraph("graph");
    private final Configuration conf = new BaseConfiguration() {{
        setProperty(Graph.GRAPH, RemoteGraph.class.getName());
        setProperty(GREMLIN_REMOTE_CONNECTION_CLASS, DriverRemoteConnection.class.getName());
        setProperty(DriverRemoteConnection.GREMLIN_REMOTE_DRIVER_SOURCENAME, "g");
        setProperty("hidden.for.testing.only", graphGetter);
        setProperty("clusterConfiguration.port", TestClientFactory.PORT);
        setProperty("clusterConfiguration.hosts", "localhost");
    }};

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
                settings.scriptEvaluationTimeout = 1000;
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
            case "shouldEnableSsl":
            case "shouldEnableSslButFailIfClientConnectsWithoutIt":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                break;
            case "shouldEnableSslWithSslContextProgrammaticallySpecified":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.overrideSslContext(createServerSslContext());
                break;
            case "shouldEnableSslAndClientCertificateAuth":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.needClientAuth = ClientAuth.REQUIRE;
                settings.ssl.keyCertChainFile = SERVER_CRT;
                settings.ssl.keyFile = SERVER_KEY;
                settings.ssl.keyPassword =KEY_PASS;
                // Trust the client
                settings.ssl.trustCertChainFile = CLIENT_CRT;
            	break;
            case "shouldEnableSslAndClientCertificateAuthAndFailWithoutCert":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.needClientAuth = ClientAuth.REQUIRE;
                settings.ssl.keyCertChainFile = SERVER_CRT;
                settings.ssl.keyFile = SERVER_KEY;
                settings.ssl.keyPassword =KEY_PASS;
                // Trust the client
                settings.ssl.trustCertChainFile = CLIENT_CRT;
            	break;
            case "shouldEnableSslAndClientCertificateAuthAndFailWithoutTrustedClientCert":
                settings.ssl = new Settings.SslSettings();
                settings.ssl.enabled = true;
                settings.ssl.needClientAuth = ClientAuth.REQUIRE;
                settings.ssl.keyCertChainFile = SERVER_CRT;
                settings.ssl.keyFile = SERVER_KEY;
                settings.ssl.keyPassword =KEY_PASS;
                // Trust ONLY the server cert
                settings.ssl.trustCertChainFile = SERVER_CRT;
            	break;
            case "shouldUseSimpleSandbox":
                settings.scriptEngines.get("gremlin-groovy").config = getScriptEngineConfForSimpleSandbox();
                break;
            case "shouldUseInterpreterMode":
                settings.scriptEngines.get("gremlin-groovy").config = getScriptEngineConfForInterpreterMode();
                break;
            case "shouldReceiveFailureTimeOutOnScriptEvalOfOutOfControlLoop":
                settings.scriptEngines.get("gremlin-groovy").config = getScriptEngineConfForTimedInterrupt();
                break;
            case "shouldUseBaseScript":
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
                settings.scriptEvaluationTimeout = 500;
                break;
        }

        return settings;
    }

    private static SslContext createServerSslContext() {
        final SslProvider provider = SslProvider.JDK;

        try {
            // this is not good for production - just testing
            final SelfSignedCertificate ssc = new SelfSignedCertificate();
            return SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).sslProvider(provider).build();
        } catch (Exception ce) {
            throw new RuntimeException("Couldn't setup self-signed certificate for test");
        }
    }

    private static Map<String, Object> getScriptEngineConfForSimpleSandbox() {
        final Map<String,Object> scriptEngineConf = new HashMap<>();
        final Map<String,Object> compilerCustomizerProviderConf = new HashMap<>();
        final List<String> sandboxes = new ArrayList<>();
        sandboxes.add(SimpleSandboxExtension.class.getName());
        compilerCustomizerProviderConf.put(CompileStaticCustomizerProvider.class.getName(), sandboxes);
        scriptEngineConf.put("compilerCustomizerProviders", compilerCustomizerProviderConf);
        return scriptEngineConf;
    }

    private static Map<String, Object> getScriptEngineConfForTimedInterrupt() {
        final Map<String,Object> scriptEngineConf = new HashMap<>();
        final Map<String,Object> timedInterruptProviderConf = new HashMap<>();
        final List<Object> config = new ArrayList<>();
        config.add(1000);
        timedInterruptProviderConf.put(TimedInterruptCustomizerProvider.class.getName(), config);
        scriptEngineConf.put("compilerCustomizerProviders", timedInterruptProviderConf);
        return scriptEngineConf;
    }

    private static Map<String, Object> getScriptEngineConfForInterpreterMode() {
        final Map<String,Object> scriptEngineConf = new HashMap<>();
        final Map<String,Object> interpreterProviderConf = new HashMap<>();
        interpreterProviderConf.put(InterpreterModeCustomizerProvider.class.getName(), Collections.EMPTY_LIST);
        scriptEngineConf.put("compilerCustomizerProviders", interpreterProviderConf);
        return scriptEngineConf;
    }

    private static Map<String, Object> getScriptEngineConfForBaseScript() {
        final Map<String,Object> scriptEngineConf = new HashMap<>();
        final Map<String,Object> compilerCustomizerProviderConf = new HashMap<>();
        final List<Object> keyValues = new ArrayList<>();

        final Map<String,Object> properties = new HashMap<>();
        properties.put("ScriptBaseClass", BaseScriptForTesting.class.getName());
        keyValues.add(properties);

        compilerCustomizerProviderConf.put(ConfigurationCustomizerProvider.class.getName(), keyValues);
        scriptEngineConf.put("compilerCustomizerProviders", compilerCustomizerProviderConf);
        return scriptEngineConf;
    }

    @Test
    public void shouldTimeOutRemoteTraversal() throws Exception {
        final Graph graph = EmptyGraph.instance();
        final GraphTraversalSource g = graph.traversal().withRemote(conf);

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
    public void shouldEnableSsl() {
        final Cluster cluster = TestClientFactory.build().enableSsl(true).create();
        final Client client = cluster.connect();

        try {
            // this should return "nothing" - there should be no exception
            assertEquals("test", client.submit("'test'").one().getString());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslWithSslContextProgrammaticallySpecified() throws Exception {
        // just for testing - this is not good for production use
        final SslContextBuilder builder = SslContextBuilder.forClient();
        builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        builder.sslProvider(SslProvider.JDK);

        final Cluster cluster = TestClientFactory.build().enableSsl(true).sslContext(builder.build()).create();
        final Client client = cluster.connect();

        try {
            // this should return "nothing" - there should be no exception
            assertEquals("test", client.submit("'test'").one().getString());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslButFailIfClientConnectsWithoutIt() {
        final Cluster cluster = TestClientFactory.build().enableSsl(false).create();
        final Client client = cluster.connect();

        try {
            client.submit("'test'").one();
            fail("Should throw exception because ssl is enabled on the server but not on client");
        } catch(Exception x) {
            final Throwable root = ExceptionUtils.getRootCause(x);
            assertThat(root, instanceOf(TimeoutException.class));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslAndClientCertificateAuth() {
		final Cluster cluster = TestClientFactory.build().enableSsl(true)
				.keyCertChainFile(CLIENT_CRT).keyFile(CLIENT_KEY)
				.keyPassword(KEY_PASS).trustCertificateChainFile(SERVER_CRT).create();
		final Client client = cluster.connect();

        try {
        	assertEquals("test", client.submit("'test'").one().getString());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslAndClientCertificateAuthAndFailWithoutCert() {
        final Cluster cluster = TestClientFactory.build().enableSsl(true).create();
        final Client client = cluster.connect();

        try {
            client.submit("'test'").one();
            fail("Should throw exception because ssl client auth is enabled on the server but client does not have a cert");
        } catch(Exception x) {
            final Throwable root = ExceptionUtils.getRootCause(x);
            assertThat(root, instanceOf(TimeoutException.class));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldEnableSslAndClientCertificateAuthAndFailWithoutTrustedClientCert() {
		final Cluster cluster = TestClientFactory.build().enableSsl(true)
				.keyCertChainFile(CLIENT_CRT).keyFile(CLIENT_KEY)
				.keyPassword(KEY_PASS).trustCertificateChainFile(SERVER_CRT).create();
		final Client client = cluster.connect();

        try {
            client.submit("'test'").one();
            fail("Should throw exception because ssl client auth is enabled on the server but does not trust client's cert");
        } catch(Exception x) {
            final Throwable root = ExceptionUtils.getRootCause(x);
            assertThat(root, instanceOf(TimeoutException.class));
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
            assertThat(responses.get(0).getStatus().getMessage(), startsWith("Script evaluation exceeded the configured 'scriptEvaluationTimeout' threshold of 1000 ms"));

            // validate that we can still send messages to the server
            assertEquals(2, ((List<Integer>) client.submit("1+1").get(0).getResult().getData()).get(0).intValue());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReceiveFailureTimeOutOnScriptEvalUsingOverride() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()) {
            final RequestMessage msg = RequestMessage.build("eval")
                    .addArg(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT, 100)
                    .addArg(Tokens.ARGS_GREMLIN, "Thread.sleep(3000);'some-stuff-that-should not return'")
                    .create();
            final List<ResponseMessage> responses = client.submit(msg);
            assertThat(responses.get(0).getStatus().getMessage(), startsWith("Script evaluation exceeded the configured 'scriptEvaluationTimeout' threshold of 100 ms"));

            // validate that we can still send messages to the server
            assertEquals(2, ((List<Integer>) client.submit("1+1").get(0).getResult().getData()).get(0).intValue());
        }
    }

    @Test
    public void shouldReceiveFailureTimeOutOnScriptEvalOfOutOfControlLoop() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()){
            // timeout configured for 1 second so the timed interrupt should trigger prior to the
            // scriptEvaluationTimeout which is at 30 seconds by default
            final List<ResponseMessage> responses = client.submit("while(true){}");
            assertThat(responses.get(0).getStatus().getMessage(), startsWith("Timeout during script evaluation triggered by TimedInterruptCustomizerProvider"));

            // validate that we can still send messages to the server
            assertEquals(2, ((List<Integer>) client.submit("1+1").get(0).getResult().getData()).get(0).intValue());
        }
    }

    /**
     * @deprecated As of release 3.2.1, replaced by tests covering {@link Settings#scriptEvaluationTimeout}.
     */
    @Test
    @SuppressWarnings("unchecked")
    @Deprecated
    public void shouldReceiveFailureTimeOutOnTotalSerialization() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()){
            final List<ResponseMessage> responses = client.submit("(0..<100000)");

            // the last message should contain the error
            assertThat(responses.get(responses.size() - 1).getStatus().getMessage(), endsWith("Serialization of the entire response exceeded the 'serializeResponseTimeout' setting"));

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
        final Cluster cluster = TestClientFactory.build().serializer(Serializers.GRAPHSON_V1D0).create();
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
            assertEquals("Connection reset by peer", root.getMessage());

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
            client.submit("1+1").all().join();
            fail();
        } catch (RuntimeException re) {
            assertThat(re.getCause().getCause() instanceof ClosedChannelException, is(true));

            //
            // should recover when the server comes back
            //

            // restart server
            this.startServer();
            // the retry interval is 1 second, wait a bit longer
            TimeUnit.SECONDS.sleep(5);

            List<Result> results = client.submit("1+1").all().join();
            assertEquals(1, results.size());
            assertEquals(2, results.get(0).getInt());

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
    public void shouldFailWithBadScriptEval() throws Exception {
        try (SimpleClient client = TestClientFactory.createWebSocketClient()) {
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "new String().doNothingAtAllBecauseThis is a syntax error").create();
            final List<ResponseMessage> responses = client.submit(request);
            assertEquals(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION, responses.get(0).getStatus().getCode());
            assertEquals(1, responses.size());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldStillSupportDeprecatedRebindingsParameterOnServer() throws Exception {
        // this test can be removed when the rebindings arg is removed
        try (SimpleClient client = TestClientFactory.createWebSocketClient()) {
            final Map<String,String> rebindings = new HashMap<>();
            rebindings.put("xyz", "graph");
            final RequestMessage request = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "xyz.addVertex('name','jason')")
                    .addArg(Tokens.ARGS_REBINDINGS, rebindings).create();
            final List<ResponseMessage> responses = client.submit(request);
            assertEquals(1, responses.size());

            final DetachedVertex v = ((ArrayList<DetachedVertex>) responses.get(0).getResult().getData()).get(0);
            assertEquals("jason", v.value("name"));
        }
    }

    @Test
    public void shouldSupportLambdasUsingWithRemote() throws Exception {
        final Graph graph = EmptyGraph.instance();
        final GraphTraversalSource g = graph.traversal().withRemote(conf);
        g.addV("person").property("age", 20).iterate();
        g.addV("person").property("age", 10).iterate();
        assertEquals(50L, g.V().hasLabel("person").map(Lambda.function("it.get().value('age') + 10")).sum().next());
    }

    @Test
    public void shouldGetSideEffectKeysUsingWithRemote() throws Exception {
        final Graph graph = EmptyGraph.instance();
        final GraphTraversalSource g = graph.traversal().withRemote(conf);
        g.addV("person").property("age", 20).iterate();
        g.addV("person").property("age", 10).iterate();
        final GraphTraversal traversal = g.V().aggregate("a").aggregate("b");
        traversal.iterate();
        final DriverRemoteTraversalSideEffects se = (DriverRemoteTraversalSideEffects) traversal.asAdmin().getSideEffects();

        // Get keys
        final Set<String> sideEffectKeys = se.keys();
        assertEquals(2, sideEffectKeys.size());

        // Get side effects
        final BulkSet aSideEffects = se.get("a");
        assertThat(aSideEffects.isEmpty(), is(false));
        final BulkSet bSideEffects = se.get("b");
        assertThat(bSideEffects.isEmpty(), is(false));

        // Should get local keys/side effects after close
        se.close();

        final Set<String> localSideEffectKeys = se.keys();
        assertEquals(2, localSideEffectKeys.size());

        final BulkSet localASideEffects = se.get("a");
        assertThat(localASideEffects.isEmpty(), is(false));

        final BulkSet localBSideEffects = se.get("b");
        assertThat(localBSideEffects.isEmpty(), is(false));
    }

    @Test
    public void shouldCloseSideEffectsUsingWithRemote() throws Exception {
        final Graph graph = EmptyGraph.instance();
        final GraphTraversalSource g = graph.traversal().withRemote(conf);
        g.addV("person").property("age", 20).iterate();
        g.addV("person").property("age", 10).iterate();
        final GraphTraversal traversal = g.V().aggregate("a").aggregate("b");
        traversal.iterate();
        final DriverRemoteTraversalSideEffects se = (DriverRemoteTraversalSideEffects) traversal.asAdmin().getSideEffects();
        final BulkSet sideEffects = se.get("a");
        assertThat(sideEffects.isEmpty(), is(false));
        se.close();

        // Can't get new side effects after close
        try {
            se.get("b");
            fail("The traversal is closed");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(IllegalStateException.class));
            assertEquals("Traversal has been closed - no new side-effects can be retrieved", ex.getMessage());
        }

        // Earlier keys should be cached locally
        final Set<String> localSideEffectKeys = se.keys();
        assertEquals(2, localSideEffectKeys.size());
        final BulkSet localSideEffects = se.get("a");
        assertThat(localSideEffects.isEmpty(), is(false));

        // Try to get side effect from server
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect();
        final Field field = DriverRemoteTraversalSideEffects.class.getDeclaredField("serverSideEffect");
        field.setAccessible(true);
        final UUID serverSideEffectId = (UUID) field.get(se);
        final Map<String, String> aliases = new HashMap<>();
        aliases.put("g", "g");
        final RequestMessage msg = RequestMessage.build(Tokens.OPS_GATHER)
                .addArg(Tokens.ARGS_SIDE_EFFECT, serverSideEffectId)
                .addArg(Tokens.ARGS_SIDE_EFFECT_KEY, "b")
                .addArg(Tokens.ARGS_ALIASES, aliases)
                .processor("traversal").create();
        boolean error;
        try {
            client.submitAsync(msg).get().one();
            error = false;
        } catch (Exception ex) {
            error = true;
        }
        assertThat(error, is(true));
    }

    @Test
    public void shouldBlockWhenGettingSideEffectKeysUsingWithRemote() throws Exception {
        final Graph graph = EmptyGraph.instance();
        final GraphTraversalSource g = graph.traversal().withRemote(conf);
        g.addV("person").property("age", 20).iterate();
        g.addV("person").property("age", 10).iterate();
        final GraphTraversal traversal = g.V().aggregate("a")
                .sideEffect(Lambda.consumer("{Thread.sleep(3000)}"))
                .aggregate("b");

        // force strategy application - if this doesn't happen then getSideEffects() returns DefaultTraversalSideEffects
        traversal.hasNext();

        // start a separate thread to iterate
        final Thread t = new Thread(traversal::iterate);
        t.start();

        // blocks here until traversal iteration is complete
        final DriverRemoteTraversalSideEffects se = (DriverRemoteTraversalSideEffects) traversal.asAdmin().getSideEffects();

        // Get keys
        final Set<String> sideEffectKeys = se.keys();
        assertEquals(2, sideEffectKeys.size());

        // Get side effects
        final BulkSet aSideEffects = se.get("a");
        assertThat(aSideEffects.isEmpty(), is(false));
        final BulkSet bSideEffects = se.get("b");
        assertThat(bSideEffects.isEmpty(), is(false));

        // Should get local keys/side effects after close
        se.close();

        final Set<String> localSideEffectKeys = se.keys();
        assertEquals(2, localSideEffectKeys.size());

        final BulkSet localASideEffects = se.get("a");
        assertThat(localASideEffects.isEmpty(), is(false));

        final BulkSet localBSideEffects = se.get("b");
        assertThat(localBSideEffects.isEmpty(), is(false));
    }

    @Test
    public void shouldBlockWhenGettingSideEffectValuesUsingWithRemote() throws Exception {
        final Graph graph = EmptyGraph.instance();
        final GraphTraversalSource g = graph.traversal().withRemote(conf);
        g.addV("person").property("age", 20).iterate();
        g.addV("person").property("age", 10).iterate();
        final GraphTraversal traversal = g.V().aggregate("a")
                .sideEffect(Lambda.consumer("{Thread.sleep(3000)}"))
                .aggregate("b");

        // force strategy application - if this doesn't happen then getSideEffects() returns DefaultTraversalSideEffects
        traversal.hasNext();

        // start a separate thread to iterate
        final Thread t = new Thread(traversal::iterate);
        t.start();

        // blocks here until traversal iteration is complete
        final DriverRemoteTraversalSideEffects se = (DriverRemoteTraversalSideEffects) traversal.asAdmin().getSideEffects();

        // Get side effects
        final BulkSet aSideEffects = se.get("a");
        assertThat(aSideEffects.isEmpty(), is(false));
        final BulkSet bSideEffects = se.get("b");
        assertThat(bSideEffects.isEmpty(), is(false));

        // Get keys
        final Set<String> sideEffectKeys = se.keys();
        assertEquals(2, sideEffectKeys.size());

        // Should get local keys/side effects after close
        se.close();

        final Set<String> localSideEffectKeys = se.keys();
        assertEquals(2, localSideEffectKeys.size());

        final BulkSet localASideEffects = se.get("a");
        assertThat(localASideEffects.isEmpty(), is(false));

        final BulkSet localBSideEffects = se.get("b");
        assertThat(localBSideEffects.isEmpty(), is(false));
    }

    @Test
    public void shouldDoNonBlockingPromiseWithRemote() throws Exception {
        final Graph graph = EmptyGraph.instance();
        final GraphTraversalSource g = graph.traversal().withRemote(conf);
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
            assertThat(ex.getMessage(), containsString("The Gremlin statement that was submitted exceed the maximum compilation size allowed by the JVM"));
        }
    }
}
