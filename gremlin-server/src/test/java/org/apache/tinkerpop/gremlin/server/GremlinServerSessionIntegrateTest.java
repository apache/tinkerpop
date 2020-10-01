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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.simple.SimpleClient;
import org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor;
import org.apache.tinkerpop.gremlin.util.Log4jRecordingAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsIterableContaining.hasItem;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServerSessionIntegrateTest extends AbstractGremlinServerIntegrationTest {
    private Log4jRecordingAppender recordingAppender = null;
    private Level originalLevel;

    @Before
    public void setupForEachTest() {
        recordingAppender = new Log4jRecordingAppender();
        final Logger rootLogger = Logger.getRootLogger();
        originalLevel = rootLogger.getLevel();
        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldCloseSessionOnceOnRequest":
            case "shouldHaveTheSessionTimeout":
            case "shouldCloseSessionOnClientClose":
                Logger.getRootLogger().setLevel(Level.INFO);
                break;
        }
        rootLogger.addAppender(recordingAppender);
    }

    @After
    public void teardownForEachTest() {
        final Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(originalLevel);
        rootLogger.removeAppender(recordingAppender);
    }

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "shouldHaveTheSessionTimeout":
            case "shouldCloseSessionOnceOnRequest":
                settings.processors.clear();
                final Settings.ProcessorSettings processorSettings = new Settings.ProcessorSettings();
                processorSettings.className = SessionOpProcessor.class.getCanonicalName();
                processorSettings.config = new HashMap<>();
                processorSettings.config.put(SessionOpProcessor.CONFIG_SESSION_TIMEOUT, 3000L);
                settings.processors.add(processorSettings);
                break;
            case "shouldCloseSessionOnClientClose":
                clearNeo4j(settings);
                break;
            case "shouldEnsureSessionBindingsAreThreadSafe":
                settings.threadPoolWorker = 2;
                break;
            case "shouldNotUseGlobalFunctionCache":
                settings.processors.clear();
                final Settings.ProcessorSettings processorSettingsForDisableFunctionCache = new Settings.ProcessorSettings();
                processorSettingsForDisableFunctionCache.className = SessionOpProcessor.class.getCanonicalName();
                processorSettingsForDisableFunctionCache.config = new HashMap<>();
                processorSettingsForDisableFunctionCache.config.put(SessionOpProcessor.CONFIG_GLOBAL_FUNCTION_CACHE_ENABLED, false);
                settings.processors.add(processorSettingsForDisableFunctionCache);
                break;
            case "shouldExecuteInSessionAndSessionlessWithoutOpeningTransactionWithSingleClient":
            case "shouldExecuteInSessionWithTransactionManagement":
            case "shouldRollbackOnEvalExceptionForManagedTransaction":
                clearNeo4j(settings);
                break;
        }

        return settings;
    }

    private static void clearNeo4j(Settings settings) {
        deleteDirectory(new File("/tmp/neo4j"));
        settings.graphs.put("graph", "conf/neo4j-empty.properties");
    }

    @Test
    public void shouldCloseSessionOnClientClose() throws Exception {
        assumeNeo4jIsPresent();

        final Cluster cluster1 = TestClientFactory.open();
        final Client client1 = cluster1.connect(name.getMethodName());
        client1.submit("x = 1").all().join();
        client1.submit("graph.addVertex()").all().join();
        client1.close();
        cluster1.close();

        assertThat(recordingAppender.getMessages(), hasItem("INFO - Skipped attempt to close open graph transactions on shouldCloseSessionOnClientClose - close was forced\n"));
        assertThat(recordingAppender.getMessages(), hasItem("INFO - Session shouldCloseSessionOnClientClose closed\n"));

        // try to reconnect to that session and make sure no state is there
        final Cluster clusterReconnect = TestClientFactory.open();
        final Client clientReconnect = clusterReconnect.connect(name.getMethodName());

        // should get an error because "x" is not defined as this is a new session
        try {
            clientReconnect.submit("x").all().join();
            fail("Should not have been successful as 'x' was only defined in the old session");
        } catch(Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root.getMessage(), startsWith("No such property"));
        }

        // the commit from client1 should not have gone through so there should be no data present.
        assertEquals(0, clientReconnect.submit("graph.traversal().V().count()").all().join().get(0).getInt());
        clusterReconnect.close();
    }

    @Test
    public void shouldUseGlobalFunctionCache() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect(name.getMethodName());

        try {
            assertEquals(3, client.submit("def addItUp(x,y){x+y};addItUp(1,2)").all().get().get(0).getInt());
            assertEquals(3, client.submit("addItUp(1,2)").all().get().get(0).getInt());
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldNotUseGlobalFunctionCache() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect(name.getMethodName());

        try {
            assertEquals(3, client.submit("def addItUp(x,y){x+y};addItUp(1,2)").all().get().get(0).getInt());
        } catch (Exception ex) {
            cluster.close();
            throw ex;
        }

        try {
            client.submit("addItUp(1,2)").all().get().get(0).getInt();
            fail("Global functions should not be cached so the call to addItUp() should fail");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root.getMessage(), startsWith("No signature of method"));
        } finally {
            cluster.close();
        }
    }

    @Test
    public void shouldNotAllowMoreThanOneClientPerSession() throws Exception {
        final Cluster cluster1 = TestClientFactory.open();
        final Client client1 = cluster1.connect(name.getMethodName());
        client1.submit("1+1").all().join();
        final Cluster cluster2 = TestClientFactory.open();
        final Client.SessionSettings sessionSettings = Client.SessionSettings.build()
                .sessionId(name.getMethodName())
                .forceClosed(true).create();
        final Client client2 = cluster2.connect(Client.Settings.build().useSession(sessionSettings).create());

        try {
            client2.submit("2+2").all().join();
            fail("Can't have more than one client connecting to the same session");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertEquals("Session shouldNotAllowMoreThanOneClientPerSession is not bound to the connecting client", root.getMessage());
        }

        cluster1.close();
        cluster2.close();
    }

    @Test
    public void shouldRollbackOnEvalExceptionForManagedTransaction() throws Exception {
        assumeNeo4jIsPresent();

        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect(name.getMethodName(), true);

        try {
            client.submit("graph.addVertex(); throw new Exception('no worky')").all().get();
            fail("Should have tossed the manually generated exception");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            ex.printStackTrace();
            assertEquals("no worky", root.getMessage());

            // just force a commit here of "something" in case there is something lingering
            client.submit("graph.addVertex(); graph.tx().commit()").all().get();
        }

        // the transaction is managed so a rollback should have executed
        assertEquals(1, client.submit("g.V().count()").all().get().get(0).getInt());
    }

    @Test
    public void shouldCloseSessionOnceOnRequest() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect(name.getMethodName());

        final ResultSet results1 = client.submit("x = [1,2,3,4,5,6,7,8,9]");
        final AtomicInteger counter = new AtomicInteger(0);
        results1.stream().map(i -> i.get(Integer.class) * 2).forEach(i -> assertEquals(counter.incrementAndGet() * 2, Integer.parseInt(i.toString())));

        final ResultSet results2 = client.submit("x[0]+1");
        assertEquals(2, results2.all().get().get(0).getInt());

        // explicitly close the session
        client.close();

        // wait past automatic session expiration
        Thread.sleep(3500);

        try {
            // the original session should be dead so this call will open a new session with the same name but fail
            // because the state is now gone - x is an invalid property
            client.submit("x[1]+2").all().get();
            fail("Session should be dead");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root, instanceOf(IllegalStateException.class));
        } finally {
            cluster.close();
        }

        assertEquals(1, recordingAppender.getMessages().stream()
                .filter(msg -> msg.equals("INFO - Session shouldCloseSessionOnceOnRequest closed\n")).count());
    }

    @Test
    public void shouldHaveTheSessionTimeout() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client client = cluster.connect(name.getMethodName());

        final ResultSet results1 = client.submit("x = [1,2,3,4,5,6,7,8,9]");
        final AtomicInteger counter = new AtomicInteger(0);
        results1.stream().map(i -> i.get(Integer.class) * 2).forEach(i -> assertEquals(counter.incrementAndGet() * 2, Integer.parseInt(i.toString())));

        final ResultSet results2 = client.submit("x[0]+1");
        assertEquals(2, results2.all().get().get(0).getInt());

        // session times out in 3 seconds
        Thread.sleep(3500);

        try {
            // the original session should be dead so this call will open a new session with the same name but fail
            // because the state is now gone - x is an invalid property
            client.submit("x[1]+2").all().get();
            fail("Session should be dead");
        } catch (Exception ex) {
            final Throwable cause = ExceptionUtils.getCause(ex);
            assertThat(cause, instanceOf(ResponseException.class));
            assertEquals(ResponseStatusCode.SERVER_ERROR_EVALUATION, ((ResponseException) cause).getResponseStatusCode());

            // validate that we can still send messages to the server
            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        } finally {
            cluster.close();
        }

        // there will be one for the timeout and a second for closing the cluster
        assertEquals(2, recordingAppender.getMessages().stream()
                .filter(msg -> msg.equals("INFO - Session shouldHaveTheSessionTimeout closed\n")).count());
    }

    @Test
    public void shouldEnsureSessionBindingsAreThreadSafe() throws Exception {
        final Cluster cluster = TestClientFactory.build().
                minInProcessPerConnection(16).maxInProcessPerConnection(64).create();
        final Client client = cluster.connect(name.getMethodName());

        try {
            client.submit("a=100;b=1000;c=10000;null").all().get();
            final int requests = 10000;
            final List<CompletableFuture<ResultSet>> futures = new ArrayList<>(requests);
            IntStream.range(0, requests).forEach(i -> {
                try {
                    futures.add(client.submitAsync("a+b+c"));
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });

            assertEquals(requests, futures.size());

            int counter = 0;
            for (CompletableFuture<ResultSet> f : futures) {
                final Result r = f.get().all().get(30000, TimeUnit.MILLISECONDS).get(0);
                assertEquals(11100, r.getInt());
                counter++;
            }

            assertEquals(requests, counter);
        } catch (Exception ex) {
            fail(ex.getMessage());
        } finally {
            cluster.close();
        }

    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldExecuteInSessionAndSessionlessWithoutOpeningTransactionWithSingleClient() throws Exception {
        assumeNeo4jIsPresent();

        try (final SimpleClient client = TestClientFactory.createWebSocketClient()) {

            //open a transaction, create a vertex, commit
            final RequestMessage openRequest = RequestMessage.build(Tokens.OPS_EVAL)
                    .processor("session")
                    .addArg(Tokens.ARGS_SESSION, name.getMethodName())
                    .addArg(Tokens.ARGS_GREMLIN, "graph.tx().open()")
                    .create();
            final List<ResponseMessage> openResponses = client.submit(openRequest);
            assertEquals(1, openResponses.size());
            assertEquals(ResponseStatusCode.SUCCESS, openResponses.get(0).getStatus().getCode());

            final RequestMessage addRequest = RequestMessage.build(Tokens.OPS_EVAL)
                    .processor("session")
                    .addArg(Tokens.ARGS_SESSION, name.getMethodName())
                    .addArg(Tokens.ARGS_GREMLIN, "v=graph.addVertex(\"name\",\"stephen\")")
                    .create();
            final List<ResponseMessage> addResponses = client.submit(addRequest);
            assertEquals(1, addResponses.size());
            assertEquals(ResponseStatusCode.SUCCESS, addResponses.get(0).getStatus().getCode());

            final RequestMessage commitRequest = RequestMessage.build(Tokens.OPS_EVAL)
                    .processor("session")
                    .addArg(Tokens.ARGS_SESSION, name.getMethodName())
                    .addArg(Tokens.ARGS_GREMLIN, "graph.tx().commit()")
                    .create();
            final List<ResponseMessage> commitResponses = client.submit(commitRequest);
            assertEquals(1, commitResponses.size());
            assertEquals(ResponseStatusCode.SUCCESS, commitResponses.get(0).getStatus().getCode());

            // Check to see if the transaction is closed.
            final RequestMessage checkRequest = RequestMessage.build(Tokens.OPS_EVAL)
                    .processor("session")
                    .addArg(Tokens.ARGS_SESSION, name.getMethodName())
                    .addArg(Tokens.ARGS_GREMLIN, "graph.tx().isOpen()")
                    .create();
            final List<ResponseMessage> checkResponses = client.submit(checkRequest);
            assertEquals(1, checkResponses.size());
            assertEquals(ResponseStatusCode.SUCCESS, checkResponses.get(0).getStatus().getCode());
            assertThat(((List<Boolean>) checkResponses.get(0).getResult().getData()).get(0), is(false));

            //lets run a sessionless read
            final RequestMessage sessionlessRequest = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "graph.traversal().V()")
                    .create();
            final List<ResponseMessage> sessionlessResponses = client.submit(sessionlessRequest);
            assertEquals(1, sessionlessResponses.size());
            assertEquals(ResponseStatusCode.SUCCESS, sessionlessResponses.get(0).getStatus().getCode());

            // Check to see if the transaction is still closed.
            final RequestMessage checkAgainRequest = RequestMessage.build(Tokens.OPS_EVAL)
                    .processor("session")
                    .addArg(Tokens.ARGS_SESSION, name.getMethodName())
                    .addArg(Tokens.ARGS_GREMLIN, "graph.tx().isOpen()")
                    .create();
            final List<ResponseMessage> checkAgainstResponses = client.submit(checkAgainRequest);
            assertEquals(1, checkAgainstResponses.size());
            assertEquals(ResponseStatusCode.SUCCESS, checkAgainstResponses.get(0).getStatus().getCode());
            assertThat(((List<Boolean>) checkAgainstResponses.get(0).getResult().getData()).get(0), is(false));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldExecuteInSessionWithTransactionManagement() throws Exception {
        assumeNeo4jIsPresent();

        try (final SimpleClient client = TestClientFactory.createWebSocketClient()) {
            final RequestMessage addRequest = RequestMessage.build(Tokens.OPS_EVAL)
                    .processor("session")
                    .addArg(Tokens.ARGS_SESSION, name.getMethodName())
                    .addArg(Tokens.ARGS_GREMLIN, "v=graph.addVertex(\"name\",\"stephen\")")
                    .addArg(Tokens.ARGS_MANAGE_TRANSACTION, true)
                    .create();
            final List<ResponseMessage> addResponses = client.submit(addRequest);
            assertEquals(1, addResponses.size());
            assertEquals(ResponseStatusCode.SUCCESS, addResponses.get(0).getStatus().getCode());

            // Check to see if the transaction is closed.
            final RequestMessage checkRequest = RequestMessage.build(Tokens.OPS_EVAL)
                    .processor("session")
                    .addArg(Tokens.ARGS_SESSION, name.getMethodName())
                    .addArg(Tokens.ARGS_GREMLIN, "graph.tx().isOpen()")
                    .create();
            final List<ResponseMessage> checkResponses = client.submit(checkRequest);
            assertEquals(1, checkResponses.size());
            assertEquals(ResponseStatusCode.SUCCESS, checkResponses.get(0).getStatus().getCode());
            assertThat(((List<Boolean>) checkResponses.get(0).getResult().getData()).get(0), is(false));

            // lets run a sessionless read and validate that the transaction was managed
            final RequestMessage sessionlessRequest = RequestMessage.build(Tokens.OPS_EVAL)
                    .addArg(Tokens.ARGS_GREMLIN, "graph.traversal().V().values('name')")
                    .create();
            final List<ResponseMessage> sessionlessResponses = client.submit(sessionlessRequest);
            assertEquals(1, sessionlessResponses.size());
            assertEquals(ResponseStatusCode.SUCCESS, sessionlessResponses.get(0).getStatus().getCode());
            assertEquals("stephen", ((List<String>) sessionlessResponses.get(0).getResult().getData()).get(0));

            // make sure the session is intact
            final RequestMessage getRequest = RequestMessage.build(Tokens.OPS_EVAL)
                    .processor("session")
                    .addArg(Tokens.ARGS_SESSION, name.getMethodName())
                    .addArg(Tokens.ARGS_GREMLIN, "v.values(\"name\")")
                    .addArg(Tokens.ARGS_MANAGE_TRANSACTION, true)
                    .create();
            final List<ResponseMessage> getResponses = client.submit(getRequest);
            assertEquals(1, getResponses.size());
            assertEquals(ResponseStatusCode.SUCCESS, getResponses.get(0).getStatus().getCode());
            assertEquals("stephen", ((List<String>) getResponses.get(0).getResult().getData()).get(0));

            // Check to see if the transaction is still closed.
            final RequestMessage checkAgainRequest = RequestMessage.build(Tokens.OPS_EVAL)
                    .processor("session")
                    .addArg(Tokens.ARGS_SESSION, name.getMethodName())
                    .addArg(Tokens.ARGS_GREMLIN, "graph.tx().isOpen()")
                    .create();
            final List<ResponseMessage> checkAgainstResponses = client.submit(checkAgainRequest);
            assertEquals(1, checkAgainstResponses.size());
            assertEquals(ResponseStatusCode.SUCCESS, checkAgainstResponses.get(0).getStatus().getCode());
            assertThat(((List<Boolean>) checkAgainstResponses.get(0).getResult().getData()).get(0), is(false));
        }
    }
}
