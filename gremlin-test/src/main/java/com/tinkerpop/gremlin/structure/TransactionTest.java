package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.AbstractGremlinTest;
import org.junit.Test;

import static com.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_STRING_VALUES;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TransactionTest extends AbstractGremlinTest {
    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldAllowAutoTransactionToWorkWithoutMutation() {
        // expecting no exceptions to be thrown here
        g.tx().commit();
        g.tx().rollback();
        g.tx().commit();
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldCommitElementAutoTransaction() {
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
    public void shouldRollbackElementAutoTransaction() {
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
    public void shouldCommitPropertyAutoTransaction() {
        final Vertex v1 = g.addVertex();
        final Edge e1 = v1.addEdge("l", v1);
        g.tx().commit();
        AbstractGremlinSuite.assertVertexEdgeCounts(1, 1);
        assertEquals(v1.getId(), g.v(v1.getId()).getId());
        assertEquals(e1.getId(), g.e(e1.getId()).getId());

        v1.setProperty("name", "marko");
        assertEquals("marko", v1.<String>getValue("name"));
        g.tx().commit();

        assertEquals("marko", v1.<String>getValue("name"));

        v1.setProperty("name", "stephen");

        assertEquals("stephen", v1.<String>getValue("name"));

        g.tx().commit();

        assertEquals("stephen", v1.<String>getValue("name"));

        e1.setProperty("name", "xxx");

        assertEquals("xxx", e1.<String>getValue("name"));

        g.tx().commit();

        assertEquals("xxx", e1.<String>getValue("name"));

        AbstractGremlinSuite.assertVertexEdgeCounts(1, 1);
        assertEquals(v1.getId(), g.v(v1.getId()).getId());
        assertEquals(e1.getId(), g.e(e1.getId()).getId());
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_STRING_VALUES)
    public void shouldRollbackPropertyAutoTransaction() {
        final Vertex v1 = g.addVertex("name", "marko");
        final Edge e1 = v1.addEdge("l", v1, "name", "xxx");
        AbstractGremlinSuite.assertVertexEdgeCounts(1, 1);
        assertEquals(v1.getId(), g.v(v1.getId()).getId());
        assertEquals(e1.getId(), g.e(e1.getId()).getId());
        assertEquals("marko", v1.<String>getValue("name"));
        assertEquals("xxx", e1.<String>getValue("name"));
        g.tx().commit();

        assertEquals("marko", v1.<String>getValue("name"));

        v1.setProperty("name", "stephen");

        assertEquals("stephen", v1.<String>getValue("name"));

        g.tx().rollback();

        assertEquals("marko", v1.<String>getValue("name"));

        e1.setProperty("name", "yyy");

        assertEquals("yyy", e1.<String>getValue("name"));

        g.tx().rollback();

        assertEquals("xxx", e1.<String>getValue("name"));

        AbstractGremlinSuite.assertVertexEdgeCounts(1, 1);
    }
}
