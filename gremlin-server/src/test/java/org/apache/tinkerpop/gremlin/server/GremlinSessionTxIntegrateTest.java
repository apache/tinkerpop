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
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.junit.Test;

import java.io.File;
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

        deleteDirectory(new File("/tmp/neo4j"));
        settings.graphs.put("graph", "conf/neo4j-empty.properties");

        switch (nameOfTest) {
            case "shouldExecuteBytecodeInSession":
                break;
        }

        return settings;
    }

    @Test
    public void shouldCommitTxBytecodeInSession() throws Exception {
        assumeNeo4jIsPresent();

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
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertEquals("Client is closed", root.getMessage());
        }

        cluster.close();
    }

    @Test
    public void shouldCommitTxBytecodeInSessionWithExplicitTransactionObject() throws Exception {
        assumeNeo4jIsPresent();

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
        assumeNeo4jIsPresent();

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
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertEquals("Client is closed", root.getMessage());
        }

        cluster.close();
    }

    @Test
    public void shouldCommitTxBytecodeInSessionOnCloseOfGtx() throws Exception {
        assumeNeo4jIsPresent();

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
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertEquals("Client is closed", root.getMessage());
        }

        cluster.close();
    }

    @Test
    public void shouldCommitTxBytecodeInSessionOnCloseTx() throws Exception {
        assumeNeo4jIsPresent();

        final Cluster cluster = TestClientFactory.build().create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));

        final GraphTraversalSource gtx = g.tx().begin();
        assertThat(gtx.tx().isOpen(), is(true));

        gtx.addV("person").iterate();
        assertEquals(1, (long) gtx.V().count().next());
        gtx.tx().close();
        assertThat(gtx.tx().isOpen(), is(false));

        // sessionless connections should still be good - close() should not affect that
        assertEquals(1, (long) g.V().count().next());

        // but the spawned gtx should be dead
        try {
            gtx.addV("software").iterate();
            fail("Should have failed since we committed the transaction");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertEquals("Client is closed", root.getMessage());
        }

        cluster.close();
    }

    @Test
    public void shouldCommitTxBytecodeInSessionReusingGtxAcrossThreads() throws Exception {
        assumeNeo4jIsPresent();

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
        assumeNeo4jIsPresent();

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
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertEquals("Client is closed", root.getMessage());
        }

        try {
            gtx1b.addV("software").iterate();
            fail("Should have failed since we committed the transaction");
        } catch (Exception ex) {
            final Throwable root = ExceptionUtils.getRootCause(ex);
            assertEquals("Client is closed", root.getMessage());
        }

        cluster.close();
    }
}
