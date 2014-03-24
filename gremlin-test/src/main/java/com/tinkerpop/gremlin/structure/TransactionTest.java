package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.AbstractGremlinTest;
import static com.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;

import com.tinkerpop.gremlin.util.function.FunctionUtils;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures.FEATURE_STRING_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures.FEATURE_FLOAT_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures.FEATURE_INTEGER_VALUES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TransactionTest extends AbstractGremlinTest {
    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldAllowAutoTransactionToWorkWithoutMutationByDefault() {
        // expecting no exceptions to be thrown here
        g.tx().commit();
        g.tx().rollback();
        g.tx().commit();
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldCommitElementAutoTransactionByDefault() {
        final Vertex v1 = g.addVertex();
        final Edge e1 = v1.addEdge("l", v1);
        AbstractGremlinSuite.assertVertexEdgeCounts(1, 1);
        assertEquals(v1.getId(), g.v(v1.getId()).getId());
        assertEquals(e1.getId(), g.e(e1.getId()).getId());
        g.tx().commit();
        AbstractGremlinSuite.assertVertexEdgeCounts(1, 1);
        assertEquals(v1.getId(), g.v(v1.getId()).getId());
        assertEquals(e1.getId(), g.e(e1.getId()).getId());

        g.v(v1.getId()).remove();
        AbstractGremlinSuite.assertVertexEdgeCounts(0, 0);
        g.tx().rollback();
        AbstractGremlinSuite.assertVertexEdgeCounts(1, 1);

        g.v(v1.getId()).remove();
        AbstractGremlinSuite.assertVertexEdgeCounts(0, 0);
        g.tx().commit();
        AbstractGremlinSuite.assertVertexEdgeCounts(0, 0);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldRollbackElementAutoTransactionByDefault() {
        final Vertex v1 = g.addVertex();
        final Edge e1 = v1.addEdge("l", v1);
        AbstractGremlinSuite.assertVertexEdgeCounts(1, 1);
        assertEquals(v1.getId(), g.v(v1.getId()).getId());
        assertEquals(e1.getId(), g.e(e1.getId()).getId());
        g.tx().rollback();
        AbstractGremlinSuite.assertVertexEdgeCounts(0, 0);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldCommitPropertyAutoTransactionByDefault() {
        final Vertex v1 = g.addVertex();
        final Edge e1 = v1.addEdge("l", v1);
        g.tx().commit();
        AbstractGremlinSuite.assertVertexEdgeCounts(1, 1);
        assertEquals(v1.getId(), g.v(v1.getId()).getId());
        assertEquals(e1.getId(), g.e(e1.getId()).getId());

        v1.setProperty("name", "marko");
        assertEquals("marko", v1.<String>getValue("name"));
        assertEquals("marko", g.v(v1.getId()).<String>getValue("name"));
        g.tx().commit();

        assertEquals("marko", v1.<String>getValue("name"));
        assertEquals("marko", g.v(v1.getId()).<String>getValue("name"));

        v1.setProperty("name", "stephen");

        assertEquals("stephen", v1.<String>getValue("name"));
        assertEquals("stephen", g.v(v1.getId()).<String>getValue("name"));

        g.tx().commit();

        assertEquals("stephen", v1.<String>getValue("name"));
        assertEquals("stephen", g.v(v1.getId()).<String>getValue("name"));

        e1.setProperty("name", "xxx");

        assertEquals("xxx", e1.<String>getValue("name"));
        assertEquals("xxx", g.e(e1.getId()).<String>getValue("name"));

        g.tx().commit();

        assertEquals("xxx", e1.<String>getValue("name"));
        assertEquals("xxx", g.e(e1.getId()).<String>getValue("name"));

        AbstractGremlinSuite.assertVertexEdgeCounts(1, 1);
        assertEquals(v1.getId(), g.v(v1.getId()).getId());
        assertEquals(e1.getId(), g.e(e1.getId()).getId());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = EdgePropertyFeatures.FEATURE_STRING_VALUES)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldRollbackPropertyAutoTransactionByDefault() {
        final Vertex v1 = g.addVertex("name", "marko");
        final Edge e1 = v1.addEdge("l", v1, "name", "xxx");
        AbstractGremlinSuite.assertVertexEdgeCounts(1, 1);
        assertEquals(v1.getId(), g.v(v1.getId()).getId());
        assertEquals(e1.getId(), g.e(e1.getId()).getId());
        assertEquals("marko", v1.<String>getValue("name"));
        assertEquals("xxx", e1.<String>getValue("name"));
        g.tx().commit();

        assertEquals("marko", v1.<String>getValue("name"));
        assertEquals("marko", g.v(v1.getId()).<String>getValue("name"));

        v1.setProperty("name", "stephen");

        assertEquals("stephen", v1.<String>getValue("name"));
        assertEquals("stephen", g.v(v1.getId()).<String>getValue("name"));

        g.tx().rollback();

        assertEquals("marko", v1.<String>getValue("name"));
        assertEquals("marko", g.v(v1.getId()).<String>getValue("name"));

        e1.setProperty("name", "yyy");

        assertEquals("yyy", e1.<String>getValue("name"));
        assertEquals("yyy", g.e(e1.getId()).<String>getValue("name"));

        g.tx().rollback();

        assertEquals("xxx", e1.<String>getValue("name"));
        assertEquals("xxx", g.e(e1.getId()).<String>getValue("name"));

        AbstractGremlinSuite.assertVertexEdgeCounts(1, 1);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldCommitOnShutdownByDefault() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Object oid = v1.getId();
        g.close();

        g = graphProvider.openTestGraph(config);
        final Vertex v2 = g.v(oid);
        assertEquals("marko", v2.<String>getValue("name"));
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldRollbackOnShutdownWhenConfigured() throws Exception {
        final Vertex v1 = g.addVertex("name", "marko");
        final Object oid = v1.getId();
        g.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);
        g.close();

        g = graphProvider.openTestGraph(config);
        try {
            g.v(oid);
            fail("Vertex should not be found as close behavior was set to rollback");
        } catch (Exception ex) {
            final Exception expected = Graph.Exceptions.elementNotFound();
            assertEquals(expected.getMessage(), ex.getMessage());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_FLOAT_VALUES)
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
                public void run() {
                    final Random random = new Random();
                    if (random.nextBoolean()) {
                        final Vertex a = graph.addVertex();
                        final Vertex b = graph.addVertex();
                        final Edge e = a.addEdge("friend", b);

                        vertices.getAndAdd(2);
                        a.setProperty("test", this.getId());
                        b.setProperty("blah", random.nextFloat());
                        e.setProperty("bloop", random.nextInt());
                        edges.getAndAdd(1);
                        graph.tx().commit();
                    } else {
                        final Vertex a = graph.addVertex();
                        final Vertex b = graph.addVertex();
                        final Edge e = a.addEdge("friend", b);

                        a.setProperty("test", this.getId());
                        b.setProperty("blah", random.nextFloat());
                        e.setProperty("bloop", random.nextInt());

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
        AbstractGremlinSuite.assertVertexEdgeCounts(vertices.get(), edges.get());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldExcecuteCompetingThreadsOnMultipleDbInstances() throws Exception {
        // the idea behind this test is to simulate a gremlin-server environment where two graphs of the same type
        // are being mutated by multiple threads. originally replicated a bug that was part of OrientDB.

        final Configuration configuration = graphProvider.newGraphConfiguration("g1");
        graphProvider.clear(null, configuration);
        final Graph g1 = graphProvider.openTestGraph(configuration);

        final Thread threadModFirstGraph = new Thread() {
            public void run() {
                g.addVertex();
                g.tx().commit();
            }
        };

        threadModFirstGraph.start();
        threadModFirstGraph.join();

        final Thread threadReadBothGraphs = new Thread() {
            public void run() {
                final long gCounter = g.V().count();
                assertEquals(1l, gCounter);

                final long g1Counter = g1.V().count();
                assertEquals(0l, g1Counter);
            }
        };

        threadReadBothGraphs.start();
        threadReadBothGraphs.join();

        // need to manually close the "g1" instance
        graphProvider.clear(g1, configuration);
    }

    @Test
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
        AbstractGremlinSuite.assertVertexEdgeCounts(0, 0);
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldSupportTransactionFireAndForget() {
        final Graph graph = g;

        // first fail the tx
        g.tx().submit(FunctionUtils.wrapFunction(grx -> {
            grx.addVertex();
            throw new Exception("fail");
        })).fireAndForget();
        AbstractGremlinSuite.assertVertexEdgeCounts(0, 0);

        // this tx will work
        g.tx().submit(grx->graph.addVertex()).fireAndForget();
        AbstractGremlinSuite.assertVertexEdgeCounts(1, 0);

        // make sure a commit happened and a new tx started
        g.tx().rollback();
        AbstractGremlinSuite.assertVertexEdgeCounts(1, 0);
    }

    @Test
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

        AbstractGremlinSuite.assertVertexEdgeCounts(0, 0);

        // this tx will work
        g.tx().submit(grx->graph.addVertex()).oneAndDone();
        AbstractGremlinSuite.assertVertexEdgeCounts(1, 0);

        // make sure a commit happened and a new tx started
        g.tx().rollback();
        AbstractGremlinSuite.assertVertexEdgeCounts(1, 0);
    }

    @Test
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
        AbstractGremlinSuite.assertVertexEdgeCounts(0, 0);

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
        AbstractGremlinSuite.assertVertexEdgeCounts(1, 0);

        // make sure a commit happened and a new tx started
        g.tx().rollback();
        AbstractGremlinSuite.assertVertexEdgeCounts(1, 0);
    }

    @Test
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
        AbstractGremlinSuite.assertVertexEdgeCounts(0, 0);

        // this tx will retry for specific tx and then fail after several tries
        final AtomicInteger setOfTries = new AtomicInteger(0);
        g.tx().submit(FunctionUtils.wrapFunction(grx -> {
            final int tryNumber = setOfTries.incrementAndGet();
            if (tryNumber == Transaction.Workload.DEFAULT_TRIES - 2)
                throw new Exception("fail");
            else
                throw new IllegalStateException("fail");
        })).exponentialBackoff(Transaction.Workload.DEFAULT_TRIES, 20, exceptions);

        assertEquals(Transaction.Workload.DEFAULT_TRIES - 2, setOfTries.get());
        AbstractGremlinSuite.assertVertexEdgeCounts(0, 0);

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
        AbstractGremlinSuite.assertVertexEdgeCounts(1, 0);

        // make sure a commit happened and a new tx started
        g.tx().rollback();
        AbstractGremlinSuite.assertVertexEdgeCounts(1, 0);
    }


}
