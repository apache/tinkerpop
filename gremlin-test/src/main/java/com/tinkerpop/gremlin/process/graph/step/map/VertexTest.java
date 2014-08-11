package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class VertexTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_v4_both();

    public abstract Traversal<Vertex, Vertex> get_g_v4_bothX1X();

    public abstract Traversal<Vertex, Vertex> get_g_v4_bothX2X();

    public abstract Traversal<Vertex, Vertex> get_g_v4_bothXcreatedX();

    public abstract Traversal<Vertex, Vertex> get_g_v4_bothXcreated_knowsX();

    public abstract Traversal<Vertex, Vertex> get_g_v4_bothX1_created_knowsX();

    public abstract Traversal<Vertex, Edge> get_g_v4_bothE();

    public abstract Traversal<Vertex, Edge> get_g_v4_bothEX1X();

    public abstract Traversal<Vertex, Edge> get_g_v4_bothEX2X();

    public abstract Traversal<Vertex, Edge> get_g_v4_bothEXcreatedX();

    public abstract Traversal<Vertex, Edge> get_g_v4_bothEXcreated_knowsX();

    public abstract Traversal<Vertex, Edge> get_g_v4_bothEX1_created_knowsX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v4_both_count() {
        assertEquals(3, get_g_v4_both().toList().size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    @org.junit.Ignore // get this to pass
    public void g_v4_bothX1X_count() {
        assertEquals(1, get_g_v4_bothX1X().toList().size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    @org.junit.Ignore // get this to pass
    public void g_v4_bothX2X_count() {
        assertEquals(2, get_g_v4_bothX2X().toList().size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v4_bothXcreatedX_count() {
        assertEquals(2, get_g_v4_bothXcreatedX().toList().size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v4_bothXcreated_knowsX_count() {
        assertEquals(3, get_g_v4_bothXcreated_knowsX().toList().size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    @org.junit.Ignore // get this to pass
    public void g_v4_bothX1_created_knowsX_count() {
        assertEquals(1, get_g_v4_bothX1_created_knowsX().toList().size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v4_bothE_count() {
        assertEquals(3, get_g_v4_bothE().toList().size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v4_bothEX1X_count() {
        assertEquals(1, get_g_v4_bothEX1X().toList().size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v4_bothEX2X_count() {
        assertEquals(2, get_g_v4_bothEX2X().toList().size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v4_bothEXcreatedX_count() {
        assertEquals(2, get_g_v4_bothEXcreatedX().toList().size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v4_bothEXcreated_knowsX_count() {
        assertEquals(3, get_g_v4_bothEXcreated_knowsX().toList().size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v4_bothEX1_created_knowsX_count() {
        assertEquals(1, get_g_v4_bothEX1_created_knowsX().toList().size());
    }

    public static class JavaVertexTest extends VertexTest {

        public JavaVertexTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_both() {
            return g.v(4).both();
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothX1X() {
            return g.v(4).both(1);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothX2X() {
            return g.v(4).both(2);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothXcreatedX() {
            return g.v(4).both("created");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothXcreated_knowsX() {
            return g.v(4).both("created", "knows");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothX1_created_knowsX() {
            return g.v(4).both(1, "created", "knows");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothE() {
            return g.v(4).bothE();
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothEX1X() {
            return g.v(4).bothE(1);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothEX2X() {
            return g.v(4).bothE(2);
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothEXcreatedX() {
            return g.v(4).bothE("created");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothEXcreated_knowsX() {
            return g.v(4).bothE("created", "knows");
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothEX1_created_knowsX() {
            return g.v(4).bothE(1, "created", "knows");
        }
    }

    public static class JavaComputerVertexTest extends VertexTest {

        public JavaComputerVertexTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_both() {
            return g.v(4).both().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothX1X() {
            return g.v(4).both(1).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothX2X() {
            return g.v(4).both(2).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothXcreatedX() {
            return g.v(4).both("created").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothXcreated_knowsX() {
            return g.v(4).both("created", "knows").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_v4_bothX1_created_knowsX() {
            return g.v(4).both(1, "created", "knows").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothE() {
            return g.v(4).bothE().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothEX1X() {
            return g.v(4).bothE(1).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothEX2X() {
            return g.v(4).bothE(2).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothEXcreatedX() {
            return g.v(4).bothE("created").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothEXcreated_knowsX() {
            return g.v(4).bothE("created", "knows").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Edge> get_g_v4_bothEX1_created_knowsX() {
            return g.v(4).bothE(1, "created", "knows").submit(g.compute());
        }
    }
}
