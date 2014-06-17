package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.map.MapTest
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GroovyMapTestImpl extends MapTest {
    public GroovyMapTestImpl() { requiresGraphComputer = false; }

    @Override
    public Traversal<Vertex, String> get_g_v1_mapXnameX(final Object v1Id) {
        return g.v(v1Id).map{ v -> v.get().value("name")};
    }

    @Override
    public Traversal<Vertex, Integer> get_g_v1_outE_label_mapXlengthX(final Object v1Id) {
        return g.v(v1Id).outE().label().map{l -> l.get().length()};
    }

    @Override
    public Traversal<Vertex, Integer> get_g_v1_out_mapXnameX_transformXlengthX(final Object v1Id) {
        return g.v(v1Id).out().map{v -> v.get().value("name")}.map{n -> n.get().toString().length()};
    }

    @Override
    public Traversal<Vertex, String> get_g_V_asXaX_out_mapXa_nameX() {
        return g.V().as("a").out().map{t -> ((Vertex) t.getPath().get("a")).value("name")}.trackPaths();
    }
}
