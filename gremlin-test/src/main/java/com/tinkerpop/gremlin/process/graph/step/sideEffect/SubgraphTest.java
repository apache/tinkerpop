package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import java.util.Arrays;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__;
import static com.tinkerpop.gremlin.structure.Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES;
import static com.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures.FEATURE_USER_SUPPLIED_IDS;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class SubgraphTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Graph> get_g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX(final Object v1Id, final Graph subgraph);

    public abstract Traversal<Vertex, String> get_g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup(final Graph subgraph);

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX() throws Exception {
        final Configuration config = graphProvider.newGraphConfiguration("subgraph", this.getClass(), name.getMethodName());
        graphProvider.clear(config);
        Graph subgraph = graphProvider.openTestGraph(config);
        /////
        Traversal<Vertex, Graph> traversal = get_g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX(convertToVertexId("marko"), subgraph);
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
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
    public void g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup() throws Exception {
        final Configuration config = graphProvider.newGraphConfiguration("subgraph", this.getClass(), name.getMethodName());
        graphProvider.clear(config);
        final Graph subgraph = graphProvider.openTestGraph(config);
        /////
        Traversal<Vertex, String> traversal = get_g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup(subgraph);
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "josh", "peter"), traversal);
        final Graph subGraph = traversal.asAdmin().getSideEffects().get("sg");
        assertVertexEdgeCounts(5, 4).accept(subGraph);

        graphProvider.clear(subgraph, config);
    }

    public static class StandardTest extends SubgraphTest {

        @Override
        public Traversal<Vertex, Graph> get_g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX(final Object v1Id, final Graph subgraph) {
            return g.V(v1Id).withSideEffect("sg", () -> subgraph).outE("knows").subgraph("sg").values("name").cap("sg");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup(final Graph subgraph) {
            return g.V().withSideEffect("sg", () -> subgraph).repeat(__.bothE("created").subgraph("sg").outV()).times(5).<String>values("name").dedup();
        }
    }
}
