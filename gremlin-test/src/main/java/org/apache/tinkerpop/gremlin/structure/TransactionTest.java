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
package org.apache.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.ExceptionCoverage;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures.FEATURE_DOUBLE_VALUES;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@ExceptionCoverage(exceptionClass = Transaction.Exceptions.class, methods = {
        "transactionAlreadyOpen",
        "threadedTransactionsNotSupported",
        "openTransactionsOnClose",
        "transactionMustBeOpenToReadWrite",
        "onCloseBehaviorCannotBeNull",
        "onReadWriteBehaviorCannotBeNull"
})
public class TransactionTest extends AbstractGremlinTest {

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    public void shouldHaveExceptionConsistencyWhenTransactionAlreadyOpen() {
        if (!g.tx().isOpen())
            g.tx().open();

        try {
            g.tx().open();
            fail("An exception should be thrown when a transaction is opened twice");
        } catch (Exception ex) {
            validateException(Transaction.Exceptions.transactionAlreadyOpen(), ex);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    public void shouldHaveExceptionConsistencyWhenTransactionOpenOnClose() {
        g.tx().onClose(Transaction.CLOSE_BEHAVIOR.MANUAL);

        if (!g.tx().isOpen())
            g.tx().open();

        try {
            graph.tx().close();
            fail("An exception should be thrown when close behavior is manual and the graph is close with an open transaction");
        } catch (Exception ex) {
            validateException(Transaction.Exceptions.openTransactionsOnClose(), ex);
        } finally {
            // rollback manually to keep the test clean
            g.tx().rollback();
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    public void shouldHaveExceptionConsistencyWhenUsingManualTransaction() {
        g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);

        try {
            graph.addVertex();
            fail("An exception should be thrown when read/write behavior is manual and no transaction is opened");
        } catch (Exception ex) {
            validateException(Transaction.Exceptions.transactionMustBeOpenToReadWrite(), ex);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    public void shouldHaveExceptionConsistencyWhenUsingManualTransactionOnCommit() {
        g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);

        try {
            g.tx().commit();
            fail("An exception should be thrown when read/write behavior is manual and no transaction is opened");
        } catch (Exception ex) {
            validateException(Transaction.Exceptions.transactionMustBeOpenToReadWrite(), ex);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    public void shouldHaveExceptionConsistencyWhenUsingManualTransactionOnRollback() {
        g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);

        try {
            g.tx().rollback();
            fail("An exception should be thrown when read/write behavior is manual and no transaction is opened");
        } catch (Exception ex) {
            validateException(Transaction.Exceptions.transactionMustBeOpenToReadWrite(), ex);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    public void shouldAllowJustCommitOnlyWithAutoTransaction() {
        // not expecting any exceptions here
        g.tx().commit();
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    public void shouldAllowJustRollbackOnlyWithAutoTransaction() {
        // not expecting any exceptions here
        g.tx().rollback();
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    public void shouldHaveExceptionConsistencyWhenOnCloseToNull() {
        try {
            g.tx().onClose(null);
            fail("An exception should be thrown when onClose behavior is set to null");
        } catch (Exception ex) {
            validateException(Transaction.Exceptions.onCloseBehaviorCannotBeNull(), ex);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    public void shouldHaveExceptionConsistencyWhenOnReadWriteToNull() {
        try {
            g.tx().onReadWrite(null);
            fail("An exception should be thrown when onClose behavior is set to null");
        } catch (Exception ex) {
            validateException(Transaction.Exceptions.onReadWriteBehaviorCannotBeNull(), ex);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldAllowAutoTransactionToWorkWithoutMutationByDefault() {
        // expecting no exceptions to be thrown here
        g.tx().commit();
        assertThat(g.tx().isOpen(), is(false));
        g.tx().rollback();
        assertThat(g.tx().isOpen(), is(false));
        g.tx().commit();
        assertThat(g.tx().isOpen(), is(false));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldNotifyTransactionListenersOnCommitSuccess() {
        final AtomicInteger count = new AtomicInteger(0);
        g.tx().addTransactionListener(s -> {
            if (s == Transaction.Status.COMMIT) count.incrementAndGet();
        });
        g.tx().commit();

        assertEquals(1, count.get());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldNotifyTransactionListenersInSameThreadOnlyOnCommitSuccess() throws Exception {
        final AtomicInteger count = new AtomicInteger(0);
        g.tx().addTransactionListener(s -> {
            if (s == Transaction.Status.COMMIT) count.incrementAndGet();
        });

        final Thread t = new Thread(() -> g.tx().commit());
        t.start();
        t.join();

        assertEquals(0, count.get());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldNotifyTransactionListenersOnRollbackSuccess() {
        final AtomicInteger count = new AtomicInteger(0);
        g.tx().addTransactionListener(s -> {
            if (s == Transaction.Status.ROLLBACK) count.incrementAndGet();
        });
        g.tx().rollback();

        assertEquals(1, count.get());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldNotifyTransactionListenersInSameThreadOnlyOnRollbackSuccess() throws Exception {
        final AtomicInteger count = new AtomicInteger(0);
        g.tx().addTransactionListener(s -> {
            if (s == Transaction.Status.ROLLBACK) count.incrementAndGet();
        });

        final Thread t = new Thread(() -> g.tx().rollback());
        t.start();
        t.join();

        assertEquals(0, count.get());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
    public void shouldCommitElementAutoTransactionByDefault() {
        final Vertex v1 = graph.addVertex();
        final Edge e1 = v1.addEdge("l", v1);
        assertVertexEdgeCounts(graph, 1, 1);
        assertEquals(v1.id(), graph.vertices(v1.id()).next().id());
        assertEquals(e1.id(), graph.edges(e1.id()).next().id());
        assertThat(g.tx().isOpen(), is(true));
        g.tx().commit();
        assertThat(g.tx().isOpen(), is(false));
        assertVertexEdgeCounts(graph, 1, 1);
        assertThat(g.tx().isOpen(), is(true));
        assertEquals(v1.id(), graph.vertices(v1.id()).next().id());
        assertEquals(e1.id(), graph.edges(e1.id()).next().id());

        graph.vertices(v1.id()).forEachRemaining(Element::remove);
        assertVertexEdgeCounts(graph, 0, 0);
        g.tx().rollback();
        assertThat(g.tx().isOpen(), is(false));
        assertVertexEdgeCounts(graph, 1, 1);
        assertThat(g.tx().isOpen(), is(true));

        graph.vertices(v1.id()).forEachRemaining(Element::remove);
        assertVertexEdgeCounts(graph, 0, 0);
        g.tx().commit();
        assertThat(g.tx().isOpen(), is(false));
        assertVertexEdgeCounts(graph, 0, 0);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldRollbackElementAutoTransactionByDefault() {
        final Vertex v1 = graph.addVertex();
        final Edge e1 = v1.addEdge("l", v1);
        assertVertexEdgeCounts(graph, 1, 1);
        assertEquals(v1.id(), graph.vertices(v1.id()).next().id());
        assertEquals(e1.id(), graph.edges(e1.id()).next().id());
        g.tx().rollback();
        assertVertexEdgeCounts(graph, 0, 0);
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldCommitPropertyAutoTransactionByDefault() {
        final Vertex v1 = graph.addVertex();
        final Edge e1 = v1.addEdge("l", v1);
        g.tx().commit();
        assertVertexEdgeCounts(graph, 1, 1);
        assertEquals(v1.id(), graph.vertices(v1.id()).next().id());
        assertEquals(e1.id(), graph.edges(e1.id()).next().id());

        v1.property(VertexProperty.Cardinality.single, "name", "marko");
        assertEquals("marko", v1.<String>value("name"));
        assertEquals("marko", graph.vertices(v1.id()).next().<String>value("name"));
        g.tx().commit();

        assertEquals("marko", v1.<String>value("name"));
        assertEquals("marko", graph.vertices(v1.id()).next().<String>value("name"));

        v1.property(VertexProperty.Cardinality.single, "name", "stephen");

        assertEquals("stephen", v1.<String>value("name"));
        assertEquals("stephen", graph.vertices(v1.id()).next().<String>value("name"));

        g.tx().commit();

        assertEquals("stephen", v1.<String>value("name"));
        assertEquals("stephen", graph.vertices(v1.id()).next().<String>value("name"));

        e1.property("name", "xxx");

        assertEquals("xxx", e1.<String>value("name"));
        assertEquals("xxx", graph.edges(e1.id()).next().<String>value("name"));

        g.tx().commit();

        assertEquals("xxx", e1.<String>value("name"));
        assertEquals("xxx", graph.edges(e1.id()).next().<String>value("name"));

        assertVertexEdgeCounts(graph, 1, 1);
        assertEquals(v1.id(), graph.vertices(v1.id()).next().id());
        assertEquals(e1.id(), graph.edges(e1.id()).next().id());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldRollbackPropertyAutoTransactionByDefault() {
        Vertex v1 = graph.addVertex("name", "marko");
        Edge e1 = v1.addEdge("l", v1, "name", "xxx");
        assertVertexEdgeCounts(graph, 1, 1);
        assertEquals(v1.id(), graph.vertices(v1.id()).next().id());
        assertEquals(e1.id(), graph.edges(e1.id()).next().id());
        assertEquals("marko", v1.<String>value("name"));
        assertEquals("xxx", e1.<String>value("name"));
        g.tx().commit();

        assertEquals("marko", v1.<String>value("name"));
        assertEquals("marko", graph.vertices(v1.id()).next().<String>value("name"));

        v1 = graph.vertices(v1.id()).next();
        v1.property(VertexProperty.Cardinality.single, "name", "stephen");

        assertEquals("stephen", v1.<String>value("name"));
        assertEquals("stephen", graph.vertices(v1.id()).next().<String>value("name"));

        g.tx().rollback();

        v1 = graph.vertices(v1.id()).next();
        assertEquals("marko", v1.<String>value("name"));
        assertEquals("marko", graph.vertices(v1.id()).next().<String>value("name"));

        e1 = graph.edges(e1.id()).next();
        e1.property("name", "yyy");

        assertEquals("yyy", e1.<String>value("name"));
        assertEquals("yyy", graph.edges(e1.id()).next().<String>value("name"));

        g.tx().rollback();

        e1 = graph.edges(e1.id()).next();
        assertEquals("xxx", e1.<String>value("name"));
        assertEquals("xxx", graph.edges(e1.id()).next().<String>value("name"));

        assertVertexEdgeCounts(graph, 1, 1);
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_PERSISTENCE)
    public void shouldCommitOnCloseWhenConfigured() throws Exception {
        final AtomicReference<Object> oid = new AtomicReference<>();
        final Thread t = new Thread(() -> {
            final Vertex v1 = graph.addVertex("name", "marko");
            g.tx().onClose(Transaction.CLOSE_BEHAVIOR.COMMIT);
            oid.set(v1.id());
            graph.tx().close();
        });
        t.start();
        t.join();

        final Vertex v2 = graph.vertices(oid.get()).next();
        assertEquals("marko", v2.<String>value("name"));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_PERSISTENCE)
    public void shouldRollbackOnCloseByDefault() throws Exception {
        final AtomicReference<Object> oid = new AtomicReference<>();
        final AtomicReference<Vertex> vid = new AtomicReference<>();
        final Thread t = new Thread(() -> {
            vid.set(graph.addVertex("name", "stephen"));
            graph.tx().commit();

            try (Transaction ignored = graph.tx()) {
                final Vertex v1 = graph.addVertex("name", "marko");
                oid.set(v1.id());
            }
        });
        t.start();
        t.join();

        final Iterator<Vertex> vertices = graph.vertices(vid.get().id());
        // this was committed
        assertThat(vertices.hasNext(), is(true));
        CloseableIterator.closeIterator(vertices);

        try {
            // this was not
            graph.vertices(oid.get()).next();
            fail("Vertex should not be found as close behavior was set to rollback");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(NoSuchElementException.class));
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_DOUBLE_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_INTEGER_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_FLOAT_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_INTEGER_VALUES)
    public void shouldExecuteWithCompetingThreads() {
        int totalThreads = 250;
        final AtomicInteger vertices = new AtomicInteger(0);
        final AtomicInteger edges = new AtomicInteger(0);
        final AtomicInteger completedThreads = new AtomicInteger(0);
        for (int i = 0; i < totalThreads; i++) {
            new Thread() {
                @Override
                public void run() {
                    final Random random = new Random();
                    if (random.nextBoolean()) {
                        final Vertex a = graph.addVertex();
                        final Vertex b = graph.addVertex();
                        final Edge e = a.addEdge("friend", b);

                        vertices.getAndAdd(2);
                        a.property(VertexProperty.Cardinality.single, "test", this.getId());
                        b.property(VertexProperty.Cardinality.single, "blah", random.nextDouble());
                        e.property("bloop", random.nextInt());
                        edges.getAndAdd(1);
                        graph.tx().commit();
                    } else {
                        final Vertex a = graph.addVertex();
                        final Vertex b = graph.addVertex();
                        final Edge e = a.addEdge("friend", b);

                        a.property(VertexProperty.Cardinality.single, "test", this.getId());
                        b.property(VertexProperty.Cardinality.single, "blah", random.nextDouble());
                        e.property("bloop", random.nextInt());

                        if (random.nextBoolean()) {
                            graph.tx().commit();
                            vertices.getAndAdd(2);
                            edges.getAndAdd(1);
                        } else {
                            graph.tx().rollback();
                        }
                    }
                    completedThreads.getAndAdd(1);
                }
            }.start();
        }

        while (completedThreads.get() < totalThreads) {
        }

        assertEquals(completedThreads.get(), 250);
        assertVertexEdgeCounts(graph, vertices.get(), edges.get());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldExecuteCompetingThreadsOnMultipleDbInstances() throws Exception {
        // the idea behind this test is to simulate a gremlin-server environment where two graphs of the same type
        // are being mutated by multiple threads. originally replicated a bug that was part of OrientDB.

        final Configuration configuration = graphProvider.newGraphConfiguration("g1", this.getClass(), name.getMethodName(), null);
        graphProvider.clear(configuration);
        final Graph g1 = graphProvider.openTestGraph(configuration);

        final Thread threadModFirstGraph = new Thread() {
            @Override
            public void run() {
                graph.addVertex();
                g.tx().commit();
            }
        };

        threadModFirstGraph.start();
        threadModFirstGraph.join();

        final Thread threadReadBothGraphs = new Thread() {
            @Override
            public void run() {
                final long gCounter = IteratorUtils.count(graph.vertices());
                assertEquals(1l, gCounter);

                final long g1Counter = IteratorUtils.count(g1.vertices());
                assertEquals(0l, g1Counter);
            }
        };

        threadReadBothGraphs.start();
        threadReadBothGraphs.join();

        // need to manually close the "g1" instance
        graphProvider.clear(g1, configuration);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldSupportTransactionIsolationCommitCheck() throws Exception {
        // the purpose of this test is to simulate gremlin server access to a graph instance, where one thread modifies
        // the graph and a separate thread cannot affect the transaction of the first
        final CountDownLatch latchCommittedInOtherThread = new CountDownLatch(1);
        final CountDownLatch latchCommitInOtherThread = new CountDownLatch(1);

        final AtomicBoolean noVerticesInFirstThread = new AtomicBoolean(false);

        // this thread starts a transaction then waits while the second thread tries to commit it.
        final Thread threadTxStarter = new Thread() {
            @Override
            public void run() {
                graph.addVertex();
                latchCommitInOtherThread.countDown();

                try {
                    latchCommittedInOtherThread.await();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }

                graph.tx().rollback();

                // there should be no vertices here
                noVerticesInFirstThread.set(!graph.vertices().hasNext());
            }
        };

        threadTxStarter.start();

        // this thread tries to commit the transaction started in the first thread above.
        final Thread threadTryCommitTx = new Thread() {
            @Override
            public void run() {
                try {
                    latchCommitInOtherThread.await();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }

                // try to commit the other transaction
                graph.tx().commit();

                latchCommittedInOtherThread.countDown();
            }
        };

        threadTryCommitTx.start();

        threadTxStarter.join();
        threadTryCommitTx.join();

        assertThat(noVerticesInFirstThread.get(), is(true));
        assertVertexEdgeCounts(graph, 0, 0);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_THREADED_TRANSACTIONS)
    public void shouldSupportMultipleThreadsOnTheSameTransaction() throws Exception {
        final int numberOfThreads = 10;
        final CountDownLatch latch = new CountDownLatch(numberOfThreads);

        final Graph threadedG = g.tx().createThreadedTx();

        for (int ix = 0; ix < numberOfThreads; ix++) {
            new Thread(() -> {
                threadedG.addVertex();
                latch.countDown();
            }).start();
        }

        latch.await(10000, TimeUnit.MILLISECONDS);

        // threaded transaction is not yet committed so g should not reflect any change
        assertVertexEdgeCounts(graph, 0, 0);
        threadedG.tx().commit();

        // there should be one vertex for each thread
        graph.tx().rollback();
        assertVertexEdgeCounts(graph, numberOfThreads, 0);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_THREADED_TRANSACTIONS)
    public void shouldOpenTxWhenThreadedTransactionIsCreated() throws Exception {
        // threaded transactions should be immediately open on creation
        final Graph threadedG = g.tx().createThreadedTx();
        assertThat(threadedG.tx().isOpen(), is(true));

        threadedG.tx().rollback();
        assertThat(threadedG.tx().isOpen(), is(false));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_THREADED_TRANSACTIONS)
    public void shouldNotReuseThreadedTransaction() throws Exception {
        final int numberOfThreads = 10;
        final CountDownLatch latch = new CountDownLatch(numberOfThreads);

        final Graph threadedG = g.tx().createThreadedTx();

        for (int ix = 0; ix < numberOfThreads; ix++) {
            new Thread(() -> {
                threadedG.addVertex();
                latch.countDown();
            }).start();
        }

        latch.await(10000, TimeUnit.MILLISECONDS);

        // threaded transaction is not yet committed so g should not reflect any change
        assertVertexEdgeCounts(graph, 0, 0);
        threadedG.tx().commit();

        // there should be one vertex for each thread
        graph.tx().rollback();
        assertVertexEdgeCounts(graph, numberOfThreads, 0);

        try {
            assertThat(threadedG.tx().isOpen(), is(false));
            threadedG.addVertex();
            fail("Shouldn't be able to re-use a threaded transaction");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(IllegalStateException.class));
        } finally {
            if (threadedG.tx().isOpen()) threadedG.tx().rollback();
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldCountVerticesEdgesOnPreTransactionCommit() {
        // see a more complex version of this test at GraphTest.shouldProperlyCountVerticesAndEdgesOnAddRemove()
        Vertex v1 = graph.addVertex();
        graph.tx().commit();

        assertVertexEdgeCounts(graph, 1, 0);

        final Vertex v2 = graph.addVertex();
        v1 = graph.vertices(v1.id()).next();
        v1.addEdge("friend", v2);

        assertVertexEdgeCounts(graph, 2, 1);

        graph.tx().commit();

        assertVertexEdgeCounts(graph, 2, 1);
    }

    @Test
    @org.junit.Ignore("Ignoring this test for now.  Perhaps it will have relelvance later. see - https://github.org/apache/tinkerpop/tinkerpop3/issues/31")
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldSupportTransactionIsolationWithSeparateThreads() throws Exception {
        // one thread modifies the graph and a separate thread reads before the transaction is committed.
        // the expectation is that the changes in the transaction are isolated to the thread that made the change
        // and the second thread should not see the change until commit() in the first thread.
        final CountDownLatch latchCommit = new CountDownLatch(1);
        final CountDownLatch latchFirstRead = new CountDownLatch(1);
        final CountDownLatch latchSecondRead = new CountDownLatch(1);

        final Thread threadMod = new Thread() {
            @Override
            public void run() {
                graph.addVertex();

                latchFirstRead.countDown();

                try {
                    latchCommit.await();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }

                graph.tx().commit();

                latchSecondRead.countDown();
            }
        };

        threadMod.start();

        final AtomicLong beforeCommitInOtherThread = new AtomicLong(0);
        final AtomicLong afterCommitInOtherThreadButBeforeRollbackInCurrentThread = new AtomicLong(0);
        final AtomicLong afterCommitInOtherThread = new AtomicLong(0);
        final Thread threadRead = new Thread() {
            @Override
            public void run() {
                try {
                    latchFirstRead.await();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }

                // reading vertex before tx from other thread is committed...should have zero vertices
                beforeCommitInOtherThread.set(IteratorUtils.count(graph.vertices()));

                latchCommit.countDown();

                try {
                    latchSecondRead.await();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }

                // tx in other thread is committed...should have one vertex.  rollback first to start a new tx
                // to get a fresh read given the commit
                afterCommitInOtherThreadButBeforeRollbackInCurrentThread.set(IteratorUtils.count(graph.vertices()));
                graph.tx().rollback();
                afterCommitInOtherThread.set(IteratorUtils.count(graph.vertices()));
            }
        };

        threadRead.start();

        threadMod.join();
        threadRead.join();

        assertEquals(0l, beforeCommitInOtherThread.get());
        assertEquals(0l, afterCommitInOtherThreadButBeforeRollbackInCurrentThread.get());
        assertEquals(1l, afterCommitInOtherThread.get());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldAllowReferenceOfVertexOutsideOfOriginalTransactionalContextManual() {
        g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
        g.tx().open();
        final Vertex v1 = graph.addVertex("name", "stephen");
        g.tx().commit();

        g.tx().open();
        assertEquals("stephen", v1.value("name"));

        g.tx().rollback();
        g.tx().open();
        assertEquals("stephen", v1.value("name"));
        g.tx().close();
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldAllowReferenceOfEdgeOutsideOfOriginalTransactionalContextManual() {
        g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
        g.tx().open();
        final Vertex v1 = graph.addVertex();
        final Edge e = v1.addEdge("self", v1, "weight", 0.5d);
        g.tx().commit();

        g.tx().open();
        assertEquals(0.5d, e.value("weight"), 0.00001d);

        g.tx().rollback();
        g.tx().open();
        assertEquals(0.5d, e.value("weight"), 0.00001d);
        g.tx().close();
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldAllowReferenceOfVertexOutsideOfOriginalTransactionalContextAuto() {
        final Vertex v1 = graph.addVertex("name", "stephen");
        g.tx().commit();

        assertEquals("stephen", v1.value("name"));

        g.tx().rollback();
        assertEquals("stephen", v1.value("name"));

    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldAllowReferenceOfEdgeOutsideOfOriginalTransactionalContextAuto() {
        final Vertex v1 = graph.addVertex();
        final Edge e = v1.addEdge("self", v1, "weight", 0.5d);
        g.tx().commit();

        assertEquals(0.5d, e.value("weight"), 0.00001d);

        g.tx().rollback();
        assertEquals(0.5d, e.value("weight"), 0.00001d);
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldAllowReferenceOfVertexIdOutsideOfOriginalThreadManual() throws Exception {
        g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
        g.tx().open();
        final Vertex v1 = graph.addVertex("name", "stephen");

        final AtomicReference<Object> id = new AtomicReference<>();
        final Thread t = new Thread(() -> {
            g.tx().open();
            id.set(v1.id());
        });

        t.start();
        t.join();

        assertEquals(v1.id(), id.get());

        g.tx().rollback();
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldAllowReferenceOfEdgeIdOutsideOfOriginalThreadManual() throws Exception {
        g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
        g.tx().open();
        final Vertex v1 = graph.addVertex();
        final Edge e = v1.addEdge("self", v1, "weight", 0.5d);

        final AtomicReference<Object> id = new AtomicReference<>();
        final Thread t = new Thread(() -> {
            g.tx().open();
            id.set(e.id());
        });

        t.start();
        t.join();

        assertEquals(e.id(), id.get());

        g.tx().rollback();
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldAllowReferenceOfVertexIdOutsideOfOriginalThreadAuto() throws Exception {
        final Vertex v1 = graph.addVertex("name", "stephen");

        final AtomicReference<Object> id = new AtomicReference<>();
        final Thread t = new Thread(() -> id.set(v1.id()));
        t.start();
        t.join();

        assertEquals(v1.id(), id.get());

        g.tx().rollback();
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldAllowReferenceOfEdgeIdOutsideOfOriginalThreadAuto() throws Exception {
        final Vertex v1 = graph.addVertex();
        final Edge e = v1.addEdge("self", v1, "weight", 0.5d);

        final AtomicReference<Object> id = new AtomicReference<>();
        final Thread t = new Thread(() -> id.set(e.id()));
        t.start();
        t.join();

        assertEquals(e.id(), id.get());

        g.tx().rollback();
    }
    
    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldNotShareTransactionReadWriteConsumersAcrossThreads() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean commitFailed = new AtomicBoolean(false);
        final AtomicBoolean commitOccurred = new AtomicBoolean(false);
        
        final Thread manualThread = new Thread(() -> {
            graph.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
            try {
                latch.await();
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
            
            try{
                graph.tx().commit();
                commitFailed.set(false);
            } catch (Exception ex) {
                commitFailed.set(true);
            }
        });
        
        manualThread.start();
        
        final Thread autoThread = new Thread(() -> {
            latch.countDown();
            try {
                graph.tx().commit();
                commitOccurred.set(true);
            } catch (Exception ex) {
                commitOccurred.set(false);
            }
        });
        
        autoThread.start();
        
        manualThread.join();
        autoThread.join();
        
        assertThat("manualThread transaction readWrite should be MANUAL and should fail to commit the transaction",
                   commitFailed.get(),
                   is(true));
        assertThat("autoThread transaction readWrite should be AUTO and should commit the transaction",
                   commitOccurred.get(), is(true));
    }
    
    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldNotShareTransactionCloseConsumersAcrossThreads() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        
        final Thread manualThread = new Thread(() -> {
            graph.tx().onClose(Transaction.CLOSE_BEHAVIOR.COMMIT);
            try {
                latch.await();
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        });
        
        manualThread.start();
        
        final Thread autoThread = new Thread(() -> {
            latch.countDown();
            graph.addVertex();
            graph.tx().close();
        });
        
        autoThread.start();
        
        manualThread.join();
        autoThread.join();
        
        assertEquals(
                "Graph should be empty. autoThread transaction.onClose() should be ROLLBACK (default)",
                0,
                IteratorUtils.count(graph.vertices())
        );
    }
}
