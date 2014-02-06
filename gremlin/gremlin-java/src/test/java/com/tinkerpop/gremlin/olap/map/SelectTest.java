package com.tinkerpop.gremlin.olap.map;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectTest extends com.tinkerpop.gremlin.test.map.SelectTest {

    private final Graph g = TinkerFactory.createClassic();

    @Test
    public void g_v1_asXaX_outXknowsX_asXbX_select() {
        // super.g_v1_asXaX_outXknowsX_asXbX_select(new GremlinResult<>(g, () -> GremlinJ.of().v("1").as("a").out("knows").as("b").select()));
    }

    @Test
    public void g_v1_asXaX_outXknowsX_asXbX_selectXnameX() {
        // super.g_v1_asXaX_outXknowsX_asXbX_selectXnameX(new GremlinResult<>(g, () -> Gremlin.of().v("1").as("a").out("knows").as("b").select(v -> ((Vertex) v).getValue("name"))));
    }

    @Test
    public void g_v1_asXaX_outXknowsX_asXbX_selectXaX() {
        // super.g_v1_asXaX_outXknowsX_asXbX_selectXaX(new GremlinResult<>(g, () -> GremlinJ.of().v("1").as("a").out("knows").as("b").select(As.of("a"))));
    }

    @Test
    public void g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX() {
        //  super.g_v1_asXaX_outXknowsX_asXbX_selectXa_nameX(new GremlinResult<>(g, () -> Gremlin.of().v("1").as("a").out("knows").as("b").select(As.of("a"), v -> ((Vertex) v).getValue("name"))));
    }
}
