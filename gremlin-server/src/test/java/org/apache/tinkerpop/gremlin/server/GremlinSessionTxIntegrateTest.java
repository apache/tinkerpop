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

import org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Integration tests for gremlin-driver and bytecode sessions.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinSessionTxIntegrateTest extends AbstractGremlinServerIntegrationTest {

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final String nameOfTest = name.getMethodName();

        settings.graphs.put("graph", "conf/tinkertransactiongraph-empty.properties");

        switch (nameOfTest) {
            case "shouldExecuteBytecodeInSession":
                break;
            case "shouldTimeoutTxBytecode":
                settings.processors.clear();

                // OpProcessor setting
                final Settings.ProcessorSettings processorSettings = new Settings.ProcessorSettings();
                processorSettings.className = SessionOpProcessor.class.getCanonicalName();
                processorSettings.config = new HashMap<>();
                processorSettings.config.put(SessionOpProcessor.CONFIG_SESSION_TIMEOUT, 3000L);
                settings.processors.add(processorSettings);

                // Unified setting
                settings.sessionLifetimeTimeout = 3000L;
                break;
        }

        return settings;
    }

    @Test
    @Ignore("TINKERPOP-2832")
    public void shouldTimeoutTxBytecode() throws Exception {

        final Cluster cluster = TestClientFactory.build().create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        GraphTraversalSource gtx = g.tx().begin();
        gtx.addV("person").addE("link").iterate();
        gtx.tx().commit();

        assertEquals(1L, g.V().count().next().longValue());
        assertEquals(1L, g.E().count().next().longValue());

        try {
            gtx = g.tx().begin();

            assertEquals(1L, gtx.V().count().next().longValue());
            assertEquals(1L, gtx.E().count().next().longValue());

            // wait long enough for the session to die
            Thread.sleep(4000);

            // the following should fail with a dead session
            gtx.V().count().iterate();
            fail("Session is dead - a new one should not reopen to server this request");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        cluster.close();
    }

    @Test
    public void shouldCommitTxBytecodeInSession() throws Exception {

        final Cluster cluster = TestClientFactory.build().create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        final GraphTraversalSource gtx = g.tx().begin();
        assertThat(gtx.tx().isOpen(), is(true));

        gtx.addV("person").iterate();
        assertEquals(1, (long) gtx.V().count().next());

        // outside the session we should be at zero
        assertEquals(0, (long) g.V().count().next());

        gtx.tx().commit();
        assertThat(gtx.tx().isOpen(), is(false));

        // sessionless connections should still be good - close() should not affect that
        assertEquals(1, (long) g.V().count().next());

        // but the spawned gtx should be dead
        try {
            gtx.addV("software").iterate();
            fail("Should have failed since we committed the transaction");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertEquals("Client is closed", root.getMessage());
        }

        cluster.close();
    }

    @Test
    public void shouldCommitTxBytecodeInSessionWithExplicitTransactionObject() throws Exception {

        final Cluster cluster = TestClientFactory.build().create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
        final Transaction tx = g.tx();
        assertThat(tx.isOpen(), is(true));

        final GraphTraversalSource gtx = tx.begin();
        gtx.addV("person").iterate();
        assertEquals(1, (long) gtx.V().count().next());
        tx.commit();
        assertThat(tx.isOpen(), is(false));

        // sessionless connections should still be good - close() should not affect that
        assertEquals(1, (long) g.V().count().next());

        cluster.close();
    }

    @Test
    public void shouldRollbackTxBytecodeInSession() throws Exception {

        final Cluster cluster = TestClientFactory.build().create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        final GraphTraversalSource gtx = g.tx().begin();
        assertThat(gtx.tx().isOpen(), is(true));

        gtx.addV("person").iterate();
        assertEquals(1, (long) gtx.V().count().next());
        gtx.tx().rollback();
        assertThat(gtx.tx().isOpen(), is(false));

        // sessionless connections should still be good - close() should not affect that
        assertEquals(0, (long) g.V().count().next());

        // but the spawned gtx should be dead
        try {
            gtx.addV("software").iterate();
            fail("Should have failed since we committed the transaction");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertEquals("Client is closed", root.getMessage());
        }

        cluster.close();
    }

    @Test
    public void shouldCommitTxBytecodeInSessionOnCloseOfGtx() throws Exception {

        final Cluster cluster = TestClientFactory.build().create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        final GraphTraversalSource gtx = g.tx().begin();
        assertThat(gtx.tx().isOpen(), is(true));

        gtx.addV("person").iterate();
        assertEquals(1, (long) gtx.V().count().next());
        gtx.close();
        assertThat(gtx.tx().isOpen(), is(false));

        // sessionless connections should still be good - close() should not affect that
        assertEquals(1, (long) g.V().count().next());

        // but the spawned gtx should be dead
        try {
            gtx.addV("software").iterate();
            fail("Should have failed since we committed the transaction");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertEquals("Client is closed", root.getMessage());
        }

        cluster.close();
    }

    @Test
    public void shouldCommitTxBytecodeInSessionOnCloseTx() throws Exception {

        final Cluster cluster = TestClientFactory.build().create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        final GraphTraversalSource gtx = g.tx().begin();
        assertThat(gtx.tx().isOpen(), is(true));

        gtx.addV("person").iterate();
        assertEquals(1, (long) gtx.V().count().next());
        gtx.close();
        assertThat(gtx.tx().isOpen(), is(false));

        // sessionless connections should still be good - close() should not affect that
        assertEquals(1, (long) g.V().count().next());

        // but the spawned gtx should be dead
        try {
            gtx.addV("software").iterate();
            fail("Should have failed since we committed the transaction");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertEquals("Client is closed", root.getMessage());
        }

        cluster.close();
    }

    @Test
    public void shouldOpenAndCloseObsceneAmountOfSessions() throws Exception {

        final Cluster cluster = TestClientFactory.build().create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        // need to open significantly more sessions that we have threads in gremlinPool. if we go too obscene on
        // OpProcessor this test will take too long
        final int numberOfSessions = isUsingUnifiedChannelizer() ? 1000 : 100;
        for (int ix = 0; ix < numberOfSessions; ix ++) {
            final Transaction tx = g.tx();
            final GraphTraversalSource gtx = tx.begin();
            try {
                final Vertex v1 = gtx.addV("person").property("pid", ix + "a").next();
                final Vertex v2 = gtx.addV("person").property("pid", ix + "b").next();
                gtx.addE("knows").from(v1).to(v2).iterate();
                tx.commit();
            } catch (Exception ex) {
                tx.rollback();
                fail("Should not expect any failures");
            } finally {
                assertThat(tx.isOpen(), is(false));
            }
        }

        // sessionless connections should still be good - close() should not affect that
        assertEquals(numberOfSessions * 2, (long) g.V().count().next());
        assertEquals(numberOfSessions, (long) g.E().count().next());

        cluster.close();
    }

    @Test
    public void shouldCommitTxBytecodeInSessionReusingGtxAcrossThreads() throws Exception {

        final ExecutorService service = Executors.newFixedThreadPool(2);

        final Cluster cluster = TestClientFactory.build().create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        final GraphTraversalSource gtx = g.tx().begin();
        assertThat(gtx.tx().isOpen(), is(true));

        final int verticesToAdd = 64;
        for (int ix = 0; ix < verticesToAdd; ix++) {
            service.submit(() -> gtx.addV("person").iterate());
        }

        service.shutdown();
        service.awaitTermination(90000, TimeUnit.MILLISECONDS);

        // outside the session we should be at zero
        assertEquals(0, (long) g.V().count().next());

        assertEquals(verticesToAdd, (long) gtx.V().count().next());
        gtx.tx().commit();
        assertThat(gtx.tx().isOpen(), is(false));

        // sessionless connections should still be good - close() should not affect that
        assertEquals(verticesToAdd, (long) g.V().count().next());

        cluster.close();
    }

    @Test
    public void shouldSpawnMultipleTraversalSourceInSameTransaction() throws Exception {

        final Cluster cluster = TestClientFactory.build().create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        final Transaction tx1 = g.tx();
        final GraphTraversalSource gtx1a = tx1.begin();
        final GraphTraversalSource gtx1b = tx1.begin();
        final Transaction tx2 = g.tx();
        final GraphTraversalSource gtx2 = tx2.begin();

        gtx1a.addV("person").iterate();
        assertEquals(1, (long) gtx1a.V().count().next());
        assertEquals(1, (long) gtx1b.V().count().next());

        // outside the session we should be at zero
        assertEquals(0, (long) g.V().count().next());
        assertEquals(0, (long) gtx2.V().count().next());

        // either can commit to end the transaction
        gtx1b.tx().commit();
        assertThat(gtx1a.tx().isOpen(), is(false));
        assertThat(gtx1b.tx().isOpen(), is(false));

        // sessionless connections should still be good - close() should not affect that
        assertEquals(1, (long) g.V().count().next());

        // but the spawned gtx should be dead
        try {
            gtx1a.addV("software").iterate();
            fail("Should have failed since we committed the transaction");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertEquals("Client is closed", root.getMessage());
        }

        try {
            gtx1b.addV("software").iterate();
            fail("Should have failed since we committed the transaction");
        } catch (Exception ex) {
            final Throwable root = ExceptionHelper.getRootCause(ex);
            assertEquals("Client is closed", root.getMessage());
        }

        cluster.close();
    }

    @Test
    public void shouldCommitRollbackInScriptUsingGremlinLang() throws Exception {
        final Cluster cluster = TestClientFactory.open();
        final Client.SessionSettings sessionSettings = Client.SessionSettings.build().
                sessionId(name.getMethodName()).
                manageTransactions(false).
                maintainStateAfterException(false).
                create();
        final Client.Settings clientSettings = Client.Settings.build().useSession(sessionSettings).create();
        final Client client = cluster.connect();
        final Client session = cluster.connect(clientSettings);

        // this test mixes calls across scriptengines - probably not a use case but interesting
        try {
            session.submit("g.addV('person')").all().get(10, TimeUnit.SECONDS);
            session.submit("g.addV('person')").all().get(10, TimeUnit.SECONDS);

            // outside of session graph should be empty still but in session we should have 2
            assertEquals(0, client.submit("g.V().count()").all().get(10, TimeUnit.SECONDS).get(0).getInt());
            assertEquals(2, session.submit("g.V().count()").all().get(10, TimeUnit.SECONDS).get(0).getInt());

            // commit whats there using gremlin-language and test again
            session.submit("g.tx().commit()", RequestOptions.build().language("gremlin-lang").create()).all().get(10, TimeUnit.SECONDS);
            assertEquals(2, client.submit("g.V().count()").all().get(10, TimeUnit.SECONDS).get(0).getInt());
            assertEquals(2, session.submit("g.V().count()").all().get(10, TimeUnit.SECONDS).get(0).getInt());

            // add one more in session and test
            session.submit("g.addV('person')").all().get(10, TimeUnit.SECONDS);
            assertEquals(2, client.submit("g.V().count()").all().get(10, TimeUnit.SECONDS).get(0).getInt());
            assertEquals(3, session.submit("g.V().count()").all().get(10, TimeUnit.SECONDS).get(0).getInt());

            // rollback the additional one and test
            session.submit("g.tx().rollback()", RequestOptions.build().language("gremlin-lang").create()).all().get(10, TimeUnit.SECONDS);
            assertEquals(2, client.submit("g.V().count()").all().get(10, TimeUnit.SECONDS).get(0).getInt());
            assertEquals(2, session.submit("g.V().count()").all().get(10, TimeUnit.SECONDS).get(0).getInt());
        } finally {
            cluster.close();
        }
    }
}
