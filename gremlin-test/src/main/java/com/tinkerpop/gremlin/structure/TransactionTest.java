package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.AbstractGremlinTest;
import org.junit.Test;

import java.util.NoSuchElementException;

import static com.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_STRING_VALUES;
import static org.junit.Assert.assertEquals;
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
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_STRING_VALUES)
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
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_STRING_VALUES)
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

}
