package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class TmpTest extends AbstractGremlinTest {
    public abstract Traversal<Vertex, String> get_g_v1_outE_subgraphXknowsX(final Object v1Id, final Graph subgraph);

    public abstract Traversal<Vertex, String> get_g_E_subgraphXcreatedX(final Graph subgraph);

    @Test
    @LoadGraphWith(CLASSIC)
    public void get_g_v1_outE_subgraphXknowsX() throws Exception {
        final Configuration config = graphProvider.newGraphConfiguration("subgraph");
        graphProvider.clear(config);
        final Graph subgraph = graphProvider.openTestGraph(config);
        get_g_v1_outE_subgraphXknowsX(convertToVertexId("marko"), subgraph).iterate();

        AbstractGremlinSuite.assertVertexEdgeCounts(3, 2).accept(subgraph);
        subgraph.E().forEach(e -> {
            assertEquals("knows", e.label());
            assertEquals("marko", e.outV().value("name").next());
            assertEquals(new Integer(29), e.outV().<Integer>value("age").next());
            assertEquals(Vertex.DEFAULT_LABEL, e.outV().label().next());

            final String name = e.inV().<String>value("name").next();
            if (name.equals("vadas"))
                assertEquals(0.5f, e.value("weight"), 0.0001f);
            else if (name.equals("josh"))
                assertEquals(1.0f, e.value("weight"), 0.0001f);
            else
                fail("There's a vertex present that should not be in the subgraph");
        });

        graphProvider.clear(subgraph, config);
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void get_g_V_inE_subgraphXcreatedX() throws Exception {
        final Configuration config = graphProvider.newGraphConfiguration("subgraph");
        graphProvider.clear(config);
        final Graph subgraph = graphProvider.openTestGraph(config);
        get_g_E_subgraphXcreatedX(subgraph).iterate();

        AbstractGremlinSuite.assertVertexEdgeCounts(5, 4).accept(subgraph);

        graphProvider.clear(subgraph, config);
    }

    public static class JavaSideEffectTest extends TmpTest {
        public Traversal<Vertex, String> get_g_v1_outE_subgraphXknowsX(final Object v1Id, final Graph subgraph) {
            return g.v(v1Id).outE().subgraph(subgraph, e -> e.label().equals("knows")).value("name");
        }

        public Traversal<Vertex, String> get_g_E_subgraphXcreatedX(final Graph subgraph) {
            return g.V().inE().subgraph(subgraph, e -> e.label().equals("created")).value("name");
        }
    }
}
