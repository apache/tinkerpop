package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class FilterTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXfalseX();

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXtrueX();

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXlang_eq_javaX();

    public abstract Traversal<Vertex, Vertex> get_g_v1_filterXage_gt_30X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_v1_out_filterXage_gt_30X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX();

	public abstract Traversal<Edge, Edge> get_g_E_filterXfalseX();

	public abstract Traversal<Edge, Edge> get_g_E_filterXtrueX();

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_filterXfalseX() {
        final Iterator<Vertex> traversal = get_g_V_filterXfalseX();
        System.out.println("Testing: " + traversal);
        assertFalse(traversal.hasNext());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_filterXtrueX() {
        final Iterator<Vertex> traversal = get_g_V_filterXtrueX();
        System.out.println("Testing: " + traversal);
        int counter = 0;
		final Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            vertices.add(traversal.next());
        }
        assertEquals(6, counter);
        assertEquals(6, vertices.size());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_filterXlang_eq_javaX() {
        final Iterator<Vertex> traversal = get_g_V_filterXlang_eq_javaX();
        System.out.println("Testing: " + traversal);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("ripple") ||
                    vertex.value("name").equals("lop"));
        }
        assertEquals(2, counter);
        assertEquals(2, vertices.size());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_filterXage_gt_30X() {
        Iterator<Vertex> traversal = get_g_v1_filterXage_gt_30X(convertToVertexId("marko"));
        System.out.println("Testing: " + traversal);
        assertFalse(traversal.hasNext());
        traversal = get_g_v1_filterXage_gt_30X(convertToVertexId("josh"));
        System.out.println("Testing: " + traversal);
        assertTrue(traversal.hasNext());
        assertEquals(Integer.valueOf(32), traversal.next().<Integer>value("age"));
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_out_filterXage_gt_30X() {
        final Iterator<Vertex> traversal = get_g_v1_out_filterXage_gt_30X(convertToVertexId("marko"));
        System.out.println("Testing: " + traversal);
        assertEquals(Integer.valueOf(32), traversal.next().<Integer>value("age"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
        final Iterator<Vertex> traversal = get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX();
        System.out.println("Testing: " + traversal);
        int counter = 0;
        Set<Vertex> vertices = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            Vertex vertex = traversal.next();
            vertices.add(vertex);
            assertTrue(vertex.value("name").equals("marko") ||
                    vertex.value("name").equals("peter"));
        }
        assertEquals(counter, 2);
        assertEquals(vertices.size(), 2);
    }

	@Test
	@LoadGraphWith(CLASSIC)
	public void g_E_filterXfalseX() {
		final Iterator<Edge> traversal = get_g_E_filterXfalseX();
		System.out.println("Testing: " + traversal);
		assertFalse(traversal.hasNext());
		assertFalse(traversal.hasNext());
	}

	@Test
	@LoadGraphWith(CLASSIC)
	public void g_E_filterXtrueX() {
		final Iterator<Edge> traversal = get_g_E_filterXtrueX();
		System.out.println("Testing: " + traversal);
		int counter = 0;
		final Set<Edge> edges = new HashSet<>();
		while (traversal.hasNext()) {
			counter++;
			edges.add(traversal.next());
		}
		assertEquals(6, counter);
		assertEquals(6, edges.size());
		assertFalse(traversal.hasNext());
	}

    public static class JavaFilterTest extends FilterTest {
        public JavaFilterTest() {
            this.requiresGraphComputer = false;
        }

		@Override
        public Traversal<Vertex, Vertex> get_g_V_filterXfalseX() {
            return g.V().filter(v -> false);
        }

		@Override
        public Traversal<Vertex, Vertex> get_g_V_filterXtrueX() {
            return g.V().filter(v -> true);
        }

		@Override
        public Traversal<Vertex, Vertex> get_g_V_filterXlang_eq_javaX() {
            return g.V().filter(v -> v.get().<String>property("lang").orElse("none").equals("java"));
        }

		@Override
        public Traversal<Vertex, Vertex> get_g_v1_filterXage_gt_30X(final Object v1Id) {
            return g.v(v1Id).filter(v -> v.get().<Integer>property("age").orElse(0) > 30);
        }

		@Override
        public Traversal<Vertex, Vertex> get_g_v1_out_filterXage_gt_30X(final Object v1Id) {
            return g.v(v1Id).out().filter(v -> v.get().<Integer>property("age").orElse(0) > 30);
        }

		@Override
        public Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
            return g.V().filter(v -> {
                final String name = v.get().value("name");
                return name.startsWith("m") || name.startsWith("p");
            });
        }

		@Override
		public Traversal<Edge, Edge> get_g_E_filterXfalseX() {
			return g.E().filter(e -> false);
		}

		@Override
		public Traversal<Edge, Edge> get_g_E_filterXtrueX() {
			return g.E().filter(e -> true);
		}
	}

    public static class JavaComputerFilterTest extends FilterTest {

        public JavaComputerFilterTest() {
            this.requiresGraphComputer = true;
        }

		@Override
        public Traversal<Vertex, Vertex> get_g_V_filterXfalseX() {
            return g.V().filter(v -> false).submit(g.compute());
        }

		@Override
        public Traversal<Vertex, Vertex> get_g_V_filterXtrueX() {
            return g.V().filter(v -> true).submit(g.compute());
        }

		@Override
        public Traversal<Vertex, Vertex> get_g_V_filterXlang_eq_javaX() {
            return g.V().filter(v -> v.get().<String>property("lang").orElse("none").equals("java")).submit(g.compute());
        }

		@Override
        public Traversal<Vertex, Vertex> get_g_v1_filterXage_gt_30X(final Object v1Id) {
            return g.v(v1Id).filter(v -> v.get().<Integer>property("age").orElse(0) > 30).submit(g.compute());
        }

		@Override
        public Traversal<Vertex, Vertex> get_g_v1_out_filterXage_gt_30X(final Object v1Id) {
            return g.v(v1Id).out().filter(v -> v.get().<Integer>property("age").orElse(0) > 30).submit(g.compute());
        }

		@Override
		public Traversal<Edge, Edge> get_g_E_filterXfalseX() {
			return g.E().filter(v -> false).submit(g.compute());
		}

		@Override
		public Traversal<Edge, Edge> get_g_E_filterXtrueX() {
			return g.E().filter(v -> true).submit(g.compute());
		}

		public Traversal<Vertex, Vertex> get_g_V_filterXname_startsWith_m_OR_name_startsWith_pX() {
            return g.V().filter(v -> {
                final String name = v.get().value("name");
                return name.startsWith("m") || name.startsWith("p");
            }).submit(g.compute());
        }
    }
}
