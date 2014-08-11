package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Edge
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GroovyVertexTestImpl extends VertexTest {
    @Override
    Traversal<Vertex, Vertex> get_g_v4_both() {
        return g.v(4).both()
    }

    @Override
    Traversal<Vertex, Vertex> get_g_v4_bothX1X() {
        return g.v(4).both(1)
    }

    @Override
    Traversal<Vertex, Vertex> get_g_v4_bothX2X() {
        return g.v(4).both(2)
    }

    @Override
    Traversal<Vertex, Vertex> get_g_v4_bothXcreatedX() {
        return g.v(4).both("created")
    }

    @Override
    Traversal<Vertex, Vertex> get_g_v4_bothXcreated_knowsX() {
        return g.v(4).both("created", "knows")
    }

    @Override
    Traversal<Vertex, Vertex> get_g_v4_bothX1_created_knowsX() {
        return g.v(4).both(1, "created", "knows")
    }

    @Override
    Traversal<Vertex, Edge> get_g_v4_bothE() {
        return g.v(4).bothE()
    }

    @Override
    Traversal<Vertex, Edge> get_g_v4_bothEX1X() {
        return g.v(4).bothE(1)
    }

    @Override
    Traversal<Vertex, Edge> get_g_v4_bothEX2X() {
        return g.v(4).bothE(2)
    }

    @Override
    Traversal<Vertex, Edge> get_g_v4_bothEXcreatedX() {
        return g.v(4).bothE("created")
    }

    @Override
    Traversal<Vertex, Edge> get_g_v4_bothEXcreated_knowsX() {
        return g.v(4).bothE("created", "knows")
    }

    @Override
    Traversal<Vertex, Edge> get_g_v4_bothEX1_created_knowsX() {
        return g.v(4).bothE(1, "created", "knows")
    }
}
