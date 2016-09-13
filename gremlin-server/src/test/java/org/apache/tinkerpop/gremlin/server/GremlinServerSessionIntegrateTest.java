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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.simple.SimpleClient;
import org.apache.tinkerpop.gremlin.driver.simple.WebSocketClient;
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
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServerSessionIntegrateTest  extends AbstractGremlinServerIntegrationTest {
    private Log4jRecordingAppender recordingAppender = null;
    private Level originalLevel;

    @Before
    public void setupForEachTest() {
        recordingAppender = new Log4jRecordingAppender();
        final Logger rootLogger = Logger.getRootLogger();
        rootLogger.addAppender(recordingAppender);
        originalLevel = rootLogger.getLevel();
    }

    @After
    public void teardownForEachTest() {
        final Logger rootLogger = Logger.getRootLogger();
        rootLogger.removeAppender(recordingAppender);
        rootLogger.setLevel(originalLevel);
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

                Logger.getRootLogger().setLevel(Level.INFO);
                break;
            case "shouldEnsureSessionBindingsAreThreadSafe":
                settings.threadPoolWorker = 2;
                break;
            case "shouldExecuteInSessionAndSessionlessWithoutOpeningTransactionWithSingleClient":
            case "shouldExecuteInSessionWithTransactionManagement":
            case "shouldRollbackOnEvalExceptionForManagedTransaction":
                deleteDirectory(new File("/tmp/neo4j"));
                settings.graphs.put("graph", "conf/neo4j-empty.properties");
                break;
        }

        return settings;
    }

    @Test
    public void shouldBlockAdditionalRequestsDuringClose() throws Exception {
        // this is sorta cobbled together a bit given limits/rules about how you can use Cluster/Client instances.
        // basically, we need one to submit the long run job and one to do the close operation that will cancel the
        // long run job. it is probably possible to do this with some low-level message manipulation but that's
        // probably not necessary
        final Cluster cluster1 = Cluster.build().create();
        final Client client1 = cluster1.connect(name.getMethodName());
        client1.submit("1+1").all().join();
        final Cluster cluster2 = Cluster.build().create();
        final Client client2 = cluster2.connect(name.getMethodName());
        client2.submit("1+1").all().join();

        final ResultSet rs = client1.submit("Thread.sleep(10000);1+1");

        client2.close();

        try {
            rs.all().join();
            fail("The close of the session on client2 should have interrupted the script sent on client1");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertThat(root.getMessage(), startsWith("Script evaluation exceeded the configured 'scriptEvaluationTimeout' threshold of 30000 ms or evaluation was otherwise cancelled directly for request"));
        }

        client1.close();

        cluster1.close();
        cluster2.close();
    }


    @Test
    public void shouldRollbackOnEvalExceptionForManagedTransaction() throws Exception {
        assumeNeo4jIsPresent();

        final Cluster cluster = Cluster.build().create();
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
        final Cluster cluster = Cluster.build().create();
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
            assertThat(root, instanceOf(ConnectionException.class));
        } finally {
            cluster.close();
        }

        assertEquals(1, recordingAppender.getMessages().stream()
                .filter(msg -> msg.equals("INFO - Session shouldCloseSessionOnceOnRequest closed\n")).count());
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
            // the original session should be dead so this call will open a new session with the same name but fail
            // because the state is now gone - x is an invalid property
            client.submit("x[1]+2").all().get();
            fail("Session should be dead");
        } catch (Exception ex) {
            final Throwable cause = ExceptionUtils.getCause(ex);
            assertThat(cause, instanceOf(ResponseException.class));
            assertEquals(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION, ((ResponseException) cause).getResponseStatusCode());

            // validate that we can still send messages to the server
            assertEquals(2, client.submit("1+1").all().join().get(0).getInt());
        } finally {
            cluster.close();
        }

        assertEquals(1, recordingAppender.getMessages().stream()
                .filter(msg -> msg.equals("INFO - Session shouldHaveTheSessionTimeout closed\n")).count());
    }

    @Test
    public void shouldEnsureSessionBindingsAreThreadSafe() throws Exception {
        final Cluster cluster = Cluster.build().minInProcessPerConnection(16).maxInProcessPerConnection(64).create();
        final Client client = cluster.connect(name.getMethodName());

        client.submitAsync("a=100;b=1000;c=10000;null");
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
        for(CompletableFuture<ResultSet> f : futures) {
            final Result r = f.get().all().get(30000, TimeUnit.MILLISECONDS).get(0);
            assertEquals(11100, r.getInt());
            counter++;
        }

        assertEquals(requests, counter);

        cluster.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldExecuteInSessionAndSessionlessWithoutOpeningTransactionWithSingleClient() throws Exception {
        assumeNeo4jIsPresent();

        try (final SimpleClient client = new WebSocketClient()) {

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

        try (final SimpleClient client = new WebSocketClient()) {
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
