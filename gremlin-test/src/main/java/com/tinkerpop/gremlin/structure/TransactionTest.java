package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.ExceptionCoverage;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.FeatureRequirementSet;
import com.tinkerpop.gremlin.util.function.FunctionUtils;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;
import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

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
            g.close();
            fail("An exception should be thrown when close behavior is manual and the graph is close with an open transaction");
        } catch (Exception ex) {
            validateException(Transaction.Exceptions.openTransactionsOnClose(), ex);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
    public void shouldHaveExceptionConsistencyWhenUsingManualTransaction() {
        g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);

        try {
            g.addVertex();
            fail("An exception should be thrown when read/write behavior is manual and no transaction is opened");
        } catch (Exception ex) {
            validateException(Transaction.Exceptions.transactionMustBeOpenToReadWrite(), ex);
        }
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
        g.tx().rollback();
        g.tx().commit();
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
    public void shouldCommitElementAutoTransactionByDefault() {
        final Vertex v1 = g.addVertex();
        final Edge e1 = v1.addEdge("l", v1);
        assertVertexEdgeCounts(1, 1);
        assertEquals(v1.id(), g.v(v1.id()).id());
        assertEquals(e1.id(), g.e(e1.id()).id());
        g.tx().commit();
        assertVertexEdgeCounts(1, 1);
        assertEquals(v1.id(), g.v(v1.id()).id());
        assertEquals(e1.id(), g.e(e1.id()).id());

        g.v(v1.id()).remove();
        assertVertexEdgeCounts(0, 0);
        g.tx().rollback();
        assertVertexEdgeCounts(1, 1);

        g.v(v1.id()).remove();
        assertVertexEdgeCounts(0, 0);
        g.tx().commit();
        assertVertexEdgeCounts(0, 0);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldRollbackElementAutoTransactionByDefault() {
        final Vertex v1 = g.addVertex();
        final Edge e1 = v1.addEdge("l", v1);
        assertVertexEdgeCounts(1, 1);
        assertEquals(v1.id(), g.v(v1.id()).id());
        assertEquals(e1.id(), g.e(e1.id()).id());
        g.tx().rollback();
        assertVertexEdgeCounts(0, 0);
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldCommitPropertyAutoTransactionByDefault() {
        final Vertex v1 = g.addVertex();
        final Edge e1 = v1.addEdge("l", v1);
        g.tx().commit();
        assertVertexEdgeCounts(1, 1);
        assertEquals(v1.id(), g.v(v1.id()).id());
        assertEquals(e1.id(), g.e(e1.id()).id());

        v1.property("name", "marko");
        assertEquals("marko", v1.<String>value("name"));
        assertEquals("marko", g.v(v1.id()).<String>value("name"));
        g.tx().commit();

        assertEquals("marko", v1.<String>value("name"));
        assertEquals("marko", g.v(v1.id()).<String>value("name"));

        v1.singleProperty("name", "stephen");

        assertEquals("stephen", v1.<String>value("name"));
        assertEquals("stephen", g.v(v1.id()).<String>value("name"));

        g.tx().commit();

        assertEquals("stephen", v1.<String>value("name"));
        assertEquals("stephen", g.v(v1.id()).<String>value("name"));

        e1.property("name", "xxx");

        assertEquals("xxx", e1.<String>value("name"));
        assertEquals("xxx", g.e(e1.id()).<String>value("name"));

        g.tx().commit();

        assertEquals("xxx", e1.<String>value("name"));
        assertEquals("xxx", g.e(e1.id()).<String>value("name"));

        assertVertexEdgeCounts(1, 1);
        assertEquals(v1.id(), g.v(v1.id()).id());
        assertEquals(e1.id(), g.e(e1.id()).id());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldRollbackPropertyAutoTransactionByDefault() {
        final Vertex v1 = g.addVertex("name", "marko");
        final Edge e1 = v1.addEdge("l", v1, "name", "xxx");
        assertVertexEdgeCounts(1, 1);
        assertEquals(v1.id(), g.v(v1.id()).id());
        assertEquals(e1.id(), g.e(e1.id()).id());
        assertEquals("marko", v1.<String>value("name"));
        assertEquals("xxx", e1.<String>value("name"));
        g.tx().commit();

        assertEquals("marko", v1.<String>value("name"));
        assertEquals("marko", g.v(v1.id()).<String>value("name"));

        v1.singleProperty("name", "stephen");

        assertEquals("stephen", v1.<String>value("name"));
        assertEquals("stephen", g.v(v1.id()).<String>value("name"));

        g.tx().rollback();

        assertEquals("marko", v1.<String>value("name"));
        assertEquals("marko", g.v(v1.id()).<String>value("name"));

        e1.property("name", "yyy");

        assertEquals("yyy", e1.<String>value("name"));
        assertEquals("yyy", g.e(e1.id()).<String>value("name"));

        g.tx().rollback();

        assertEquals("xxx", e1.<String>value("name"));
        assertEquals("xxx", g.e(e1.id()).<String>value("name"));

        assertVertexEdgeCounts(1, 1);
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldCommitOnShutdownByDefault() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Object oid = v1.id();
        g.close();

        g = graphProvider.openTestGraph(config);
        final Vertex v2 = g.v(oid);
        assertEquals("marko", v2.<String>value("name"));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldRollbackOnShutdownWhenConfigured() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Object oid = v1.id();
        g.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);
        g.close();

        g = graphProvider.openTestGraph(config);
        try {
            g.v(oid);
            fail("Vertex should not be found as close behavior was set to rollback");
        } catch (Exception ex) {
            validateException(Graph.Exceptions.elementNotFound(Vertex.class, oid), ex);
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
        final Graph graph = g;
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
                        a.property("test", this.getId());
                        b.property("blah", random.nextDouble());
                        e.property("bloop", random.nextInt());
                        edges.getAndAdd(1);
                        graph.tx().commit();
                    } else {
                        final Vertex a = graph.addVertex();
                        final Vertex b = graph.addVertex();
                        final Edge e = a.addEdge("friend", b);

                        a.property("test", this.getId());
                        b.property("blah", random.nextDouble());
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
        assertVertexEdgeCounts(vertices.get(), edges.get());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldExcecuteCompetingThreadsOnMultipleDbInstances() throws Exception {
        // the idea behind this test is to simulate a gremlin-server environment where two graphs of the same type
        // are being mutated by multiple threads. originally replicated a bug that was part of OrientDB.

        final Configuration configuration = graphProvider.newGraphConfiguration("g1", this.getClass(), name.getMethodName());
        graphProvider.clear(configuration);
        final Graph g1 = graphProvider.openTestGraph(configuration);

        final Thread threadModFirstGraph = new Thread() {
            @Override
            public void run() {
                g.addVertex();
                g.tx().commit();
            }
        };

        threadModFirstGraph.start();
        threadModFirstGraph.join();

        final Thread threadReadBothGraphs = new Thread() {
            @Override
            public void run() {
                final long gCounter = g.V().count().next();
                assertEquals(1l, gCounter);

                final long g1Counter = g1.V().count().next();
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
        final Graph graph = g;

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
                noVerticesInFirstThread.set(!graph.V().hasNext());
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

        assertTrue(noVerticesInFirstThread.get());
        assertVertexEdgeCounts(0, 0);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldSupportTransactionFireAndForget() {
        final Graph graph = g;

        // first fail the tx
        g.tx().submit(FunctionUtils.wrapFunction(grx -> {
            grx.addVertex();
            throw new Exception("fail");
        })).fireAndForget();
        assertVertexEdgeCounts(0, 0);

        // this tx will work
        g.tx().submit(grx -> graph.addVertex()).fireAndForget();
        assertVertexEdgeCounts(1, 0);

        // make sure a commit happened and a new tx started
        g.tx().rollback();
        assertVertexEdgeCounts(1, 0);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldSupportTransactionOneAndDone() {
        final Graph graph = g;

        // first fail the tx
        try {
            g.tx().submit(FunctionUtils.wrapFunction(grx -> {
                grx.addVertex();
                throw new Exception("fail");
            })).oneAndDone();
        } catch (Exception ex) {
            assertEquals("fail", ex.getCause().getCause().getMessage());
        }

        assertVertexEdgeCounts(0, 0);

        // this tx will work
        g.tx().submit(grx -> graph.addVertex()).oneAndDone();
        assertVertexEdgeCounts(1, 0);

        // make sure a commit happened and a new tx started
        g.tx().rollback();
        assertVertexEdgeCounts(1, 0);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldSupportTransactionExponentialBackoff() {
        final Graph graph = g;

        // first fail the tx
        final AtomicInteger attempts = new AtomicInteger(0);
        try {
            g.tx().submit(FunctionUtils.wrapFunction(grx -> {
                grx.addVertex();
                attempts.incrementAndGet();
                throw new Exception("fail");
            })).exponentialBackoff();
        } catch (Exception ex) {
            assertEquals("fail", ex.getCause().getCause().getMessage());
        }

        assertEquals(Transaction.Workload.DEFAULT_TRIES, attempts.get());
        assertVertexEdgeCounts(0, 0);

        // this tx will work after several tries
        final AtomicInteger tries = new AtomicInteger(0);
        g.tx().submit(FunctionUtils.wrapFunction(grx -> {
            final int tryNumber = tries.incrementAndGet();
            if (tryNumber == Transaction.Workload.DEFAULT_TRIES - 2)
                return graph.addVertex();
            else
                throw new Exception("fail");
        })).exponentialBackoff();

        assertEquals(Transaction.Workload.DEFAULT_TRIES - 2, tries.get());
        assertVertexEdgeCounts(1, 0);

        // make sure a commit happened and a new tx started
        g.tx().rollback();
        assertVertexEdgeCounts(1, 0);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldSupportTransactionExponentialBackoffWithExceptionChecks() {
        final Graph graph = g;
        final Set<Class> exceptions = new HashSet<Class>() {{
            add(IllegalStateException.class);
        }};

        // first fail the tx
        final AtomicInteger attempts = new AtomicInteger(0);
        try {
            g.tx().submit(FunctionUtils.wrapFunction(grx -> {
                grx.addVertex();
                attempts.incrementAndGet();
                throw new Exception("fail");
            })).exponentialBackoff(Transaction.Workload.DEFAULT_TRIES, 20, exceptions);
        } catch (Exception ex) {
            assertEquals("fail", ex.getCause().getCause().getMessage());
        }

        assertEquals(1, attempts.get());
        assertVertexEdgeCounts(0, 0);

        // this tx will retry for specific tx and then fail after several tries
        final AtomicInteger setOfTries = new AtomicInteger(0);
        try {
            g.tx().submit(FunctionUtils.wrapFunction(grx -> {
                final int tryNumber = setOfTries.incrementAndGet();
                if (tryNumber == Transaction.Workload.DEFAULT_TRIES - 2)
                    throw new Exception("fail");
                else
                    throw new IllegalStateException("fail");
            })).exponentialBackoff(Transaction.Workload.DEFAULT_TRIES, 20, exceptions);
        } catch (Exception ex) {
            assertEquals("fail", ex.getCause().getCause().getMessage());
        }

        assertEquals(Transaction.Workload.DEFAULT_TRIES - 2, setOfTries.get());
        assertVertexEdgeCounts(0, 0);

        // this tx will work after several tries
        final AtomicInteger tries = new AtomicInteger(0);
        g.tx().submit(FunctionUtils.wrapFunction(grx -> {
            final int tryNumber = tries.incrementAndGet();
            if (tryNumber == Transaction.Workload.DEFAULT_TRIES - 2)
                return graph.addVertex();
            else
                throw new IllegalStateException("fail");
        })).exponentialBackoff(Transaction.Workload.DEFAULT_TRIES, 20, exceptions);

        assertEquals(Transaction.Workload.DEFAULT_TRIES - 2, tries.get());
        assertVertexEdgeCounts(1, 0);

        // make sure a commit happened and a new tx started
        g.tx().rollback();
        assertVertexEdgeCounts(1, 0);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldSupportTransactionRetry() {
        final Graph graph = g;

        // first fail the tx
        final AtomicInteger attempts = new AtomicInteger(0);
        try {
            g.tx().submit(FunctionUtils.wrapFunction(grx -> {
                grx.addVertex();
                attempts.incrementAndGet();
                throw new Exception("fail");
            })).retry();
        } catch (Exception ex) {
            assertEquals("fail", ex.getCause().getCause().getMessage());
        }

        assertEquals(Transaction.Workload.DEFAULT_TRIES, attempts.get());
        assertVertexEdgeCounts(0, 0);

        // this tx will work after several tries
        final AtomicInteger tries = new AtomicInteger(0);
        g.tx().submit(FunctionUtils.wrapFunction(grx -> {
            final int tryNumber = tries.incrementAndGet();
            if (tryNumber == Transaction.Workload.DEFAULT_TRIES - 2)
                return graph.addVertex();
            else
                throw new Exception("fail");
        })).retry();

        assertEquals(Transaction.Workload.DEFAULT_TRIES - 2, tries.get());
        assertVertexEdgeCounts(1, 0);

        // make sure a commit happened and a new tx started
        g.tx().rollback();
        assertVertexEdgeCounts(1, 0);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldCountVerticesEdgesOnPreTransactionCommit() {
        // see a more complex version of this test at GraphTest.shouldProperlyCountVerticesAndEdgesOnAddRemove()
        final Graph graph = g;
        Vertex v1 = graph.addVertex();
        graph.tx().commit();

        assertVertexEdgeCounts(1, 0);

        final Vertex v2 = graph.addVertex();
        v1 = graph.v(v1.id());
        v1.addEdge("friend", v2);

        assertVertexEdgeCounts(2, 1);

        graph.tx().commit();

        assertVertexEdgeCounts(2, 1);
    }

    @Test
    @org.junit.Ignore("Ignoring this test for now.  Perhaps it will have relelvance later. see - https://github.com/tinkerpop/tinkerpop3/issues/31")
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldSupportTransactionIsolationWithSeparateThreads() throws Exception {
        // one thread modifies the graph and a separate thread reads before the transaction is committed.
        // the expectation is that the changes in the transaction are isolated to the thread that made the change
        // and the second thread should not see the change until commit() in the first thread.
        final Graph graph = g;

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
                beforeCommitInOtherThread.set(graph.V().count().next());

                latchCommit.countDown();

                try {
                    latchSecondRead.await();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }

                // tx in other thread is committed...should have one vertex.  rollback first to start a new tx
                // to get a fresh read given the commit
                afterCommitInOtherThreadButBeforeRollbackInCurrentThread.set(graph.V().count().next());
                graph.tx().rollback();
                afterCommitInOtherThread.set(graph.V().count().next());
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
    public void shouldAllowReferenceOfVertexOutsideOfOrginialTransactionalContextManual() {
        g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
        g.tx().open();
        final Vertex v1 = g.addVertex("name", "stephen");
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
    public void shouldAllowReferenceOfEdgeOutsideOfOrginialTransactionalContextManual() {
        g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
        g.tx().open();
        final Vertex v1 = g.addVertex();
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
    public void shouldAllowReferenceOfVertexOutsideOfOrginialTransactionalContextAuto() {
        final Vertex v1 = g.addVertex("name", "stephen");
        g.tx().commit();

        assertEquals("stephen", v1.value("name"));

        g.tx().rollback();
        assertEquals("stephen", v1.value("name"));

    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldAllowReferenceOfEdgeOutsideOfOrginialTransactionalContextAuto() {
        final Vertex v1 = g.addVertex();
        final Edge e = v1.addEdge("self", v1, "weight", 0.5d);
        g.tx().commit();

        assertEquals(0.5d, e.value("weight"), 0.00001d);

        g.tx().rollback();
        assertEquals(0.5d, e.value("weight"), 0.00001d);
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldAllowReferenceOfVertexIdOutsideOfOrginialThreadManual() throws Exception {
        g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
        g.tx().open();
        final Vertex v1 = g.addVertex("name", "stephen");

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
    public void shouldAllowReferenceOfEdgeIdOutsideOfOrginialThreadManual() throws Exception {
        g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);
        g.tx().open();
        final Vertex v1 = g.addVertex();
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
    public void shouldAllowReferenceOfVertexIdOutsideOfOrginialThreadAuto() throws Exception  {
        final Vertex v1 = g.addVertex("name", "stephen");

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
    public void shouldAllowReferenceOfEdgeIdOutsideOfOrginialThreadAuto() throws Exception  {
        final Vertex v1 = g.addVertex();
        final Edge e = v1.addEdge("self", v1, "weight", 0.5d);

        final AtomicReference<Object> id = new AtomicReference<>();
        final Thread t = new Thread(() -> id.set(e.id()));
        t.start();
        t.join();

        assertEquals(e.id(), id.get());

        g.tx().rollback();
    }
}
