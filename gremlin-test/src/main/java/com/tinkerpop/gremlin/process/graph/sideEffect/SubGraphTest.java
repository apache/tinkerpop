package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.AbstractGremlinSuite;
import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class SubGraphTest extends AbstractGremlinTest {
    public abstract Traversal<Vertex, String> get_g_v1_outE_subgraphXknowsX(final Object v1Id, final Graph subGraph);
    public abstract Traversal<Vertex, String> get_g_E_subgraphXcreatedX(final Graph subGraph);

    @Test
    @LoadGraphWith(CLASSIC)
    public void get_g_v1_outE_subgraphXknowsX() throws Exception {
        final Configuration config = graphProvider.newGraphConfiguration("subgraph");
        graphProvider.clear(config);
        final Graph subgraph = graphProvider.openTestGraph(config);
        get_g_v1_outE_subgraphXknowsX(convertToId("marko"), subgraph).iterate();

        AbstractGremlinSuite.assertVertexEdgeCounts(3, 2).accept(subgraph);
        subgraph.E().forEach(e -> {
            assertEquals("knows", e.getLabel());
            assertEquals("marko", e.getVertex(Direction.OUT).<String>getValue("name"));
            assertEquals(new Integer(29), e.getVertex(Direction.OUT).<Integer>getValue("age"));
            assertEquals(Element.DEFAULT_LABEL, e.getVertex(Direction.OUT).getLabel());

            final String name = e.getVertex(Direction.IN).<String>getValue("name");
            if (name.equals("vadas"))
                assertEquals(0.5f, e.getValue("weight"), 0.0001f);
            else if (name.equals("josh"))
                assertEquals(1.0f, e.getValue("weight"), 0.0001f);
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

    public static class JavaSideEffectTest extends SubGraphTest {
        public Traversal<Vertex, String> get_g_v1_outE_subgraphXknowsX(final Object v1Id, final Graph subGraph) {
            return g.v(v1Id).outE().subGraph(subGraph, e -> e.getLabel().equals("knows")).value("name");
        }

        public Traversal<Vertex, String> get_g_E_subgraphXcreatedX(final Graph subGraph) {
            return g.V().inE().subGraph(subGraph, e -> e.getLabel().equals("created")).value("name");
        }
    }
}
