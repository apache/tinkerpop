package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class SubgraphTest extends AbstractGremlinTest {
    public abstract Traversal<Vertex, Graph> get_g_VX1X_outE_subgraphXknowsX_name_capXsgX(final Object v1Id, final Graph subgraph);

    public abstract Traversal<Vertex, String> get_g_V_inE_subgraphXcreatedX_name(final Graph subgraph);

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    public void g_VX1X_outE_subgraphXknowsX_name_capXsgX() throws Exception {
        final Configuration config = graphProvider.newGraphConfiguration("subgraph", this.getClass(), name.getMethodName());
        graphProvider.clear(config);
        Graph subgraph = graphProvider.openTestGraph(config);
        Traversal<Vertex, Graph> traversal = get_g_VX1X_outE_subgraphXknowsX_name_capXsgX(convertToVertexId("marko"), subgraph);
        printTraversalForm(traversal);
        subgraph = traversal.next();
        assertVertexEdgeCounts(3, 2).accept(subgraph);
        subgraph.E().forEachRemaining(e -> {
            assertEquals("knows", e.label());
            assertEquals("marko", e.outV().values("name").next());
            assertEquals(new Integer(29), e.outV().<Integer>values("age").next());
            assertEquals("person", e.outV().label().next());

            final String name = e.inV().<String>values("name").next();
            if (name.equals("vadas"))
                assertEquals(0.5d, e.value("weight"), 0.0001d);
            else if (name.equals("josh"))
                assertEquals(1.0d, e.value("weight"), 0.0001d);
            else
                fail("There's a vertex present that should not be in the subgraph");
        });

        graphProvider.clear(subgraph, config);
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    public void g_V_inE_subgraphXcreatedX_name() throws Exception {
        final Configuration config = graphProvider.newGraphConfiguration("subgraph", this.getClass(), name.getMethodName());
        graphProvider.clear(config);
        final Graph subgraph = graphProvider.openTestGraph(config);
        Traversal<Vertex, String> traversal = get_g_V_inE_subgraphXcreatedX_name(subgraph);
        printTraversalForm(traversal);
        traversal.iterate();

        assertVertexEdgeCounts(5, 4).accept(traversal.asAdmin().getSideEffects().get("sg"));

        graphProvider.clear(subgraph, config);
    }

    public static class StandardTest extends SubgraphTest {

        @Override
        public Traversal<Vertex, Graph> get_g_VX1X_outE_subgraphXknowsX_name_capXsgX(final Object v1Id, final Graph subgraph) {
            return g.V(v1Id).withSideEffect("sg", () -> subgraph).outE().subgraph("sg", e -> e.label().equals("knows")).values("name").cap("sg");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_inE_subgraphXcreatedX_name(final Graph subgraph) {
            return g.V().withSideEffect("sg", () -> subgraph).inE().subgraph("sg", e -> e.label().equals("created")).values("name");
        }
    }
}
