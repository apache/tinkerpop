package com.tinkerpop.gremlin.process.graph.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class HasNotTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_v1_hasNotXprop(Object v1Id, String prop);

    public abstract Traversal<Vertex, Vertex> get_g_V_hasNotXprop(String prop);

    @Test
    @LoadGraphWith(CLASSIC)
    public void get_g_v1_hasNotXprop() {
        Iterator<Vertex> traversal = get_g_v1_hasNotXprop(convertToId("marko"), "circumference");
        System.out.println("Testing: " + traversal);
        assertEquals("marko", traversal.next().<String>getValue("name"));
        assertFalse(traversal.hasNext());
        traversal = get_g_v1_hasNotXprop(convertToId("marko"), "name");
        System.out.println("Testing: " + traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    public void get_g_V_hasNotXprop() {
        Iterator<Vertex> traversal = get_g_V_hasNotXprop("circumference");
        System.out.println("Testing: " + traversal);
        final List<Element> list = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(6, list.size());
    }

    public static class JavaHasTest extends HasNotTest {
        public JavaHasTest() {
            requiresGraphComputer = false;
        }

        public Traversal<Vertex, Vertex> get_g_v1_hasNotXprop(final Object v1Id, final String prop) {
            return g.v(v1Id).hasNot(prop);
        }

        public Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String prop) {
            return g.V().hasNot(prop);
        }
    }

    public static class JavaComputerHasTest extends HasNotTest {
        public JavaComputerHasTest() {
            requiresGraphComputer = true;
        }

        public Traversal<Vertex, Vertex> get_g_v1_hasNotXprop(final Object v1Id, final String prop) {
            return g.v(v1Id).<Vertex>hasNot(prop).submit(g.compute());
        }

        public Traversal<Vertex, Vertex> get_g_V_hasNotXprop(final String prop) {
            return g.V().<Vertex>hasNot(prop).submit(g.compute());
        }
    }
}