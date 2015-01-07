package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class HasNotTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_hasNotXprop(final Object v1Id, final String propertyKey);

    public abstract Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String propertyKey);

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_hasNotXprop() {
        Traversal<Vertex, Vertex> traversal = get_g_VX1X_hasNotXprop(convertToVertexId("marko"), "circumference");
        printTraversalForm(traversal);
        assertEquals("marko", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
        traversal = get_g_VX1X_hasNotXprop(convertToVertexId("marko"), "name");
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void get_g_V_hasNotXprop() {
        Traversal<Vertex, Vertex> traversal = get_g_V_hasNotXprop("circumference");
        printTraversalForm(traversal);
        final List<Element> list = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(6, list.size());
    }

    public static class StandardTest extends HasNotTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasNotXprop(final Object v1Id, final String propertyKey) {
            return g.V(v1Id).hasNot(propertyKey);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String propertyKey) {
            return g.V().hasNot(propertyKey);
        }
    }

    public static class ComputerTest extends HasNotTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasNotXprop(final Object v1Id, final String propertyKey) {
            return g.V(v1Id).<Vertex>hasNot(propertyKey).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String propertyKey) {
            return g.V().<Vertex>hasNot(propertyKey).submit(g.compute());
        }
    }
}