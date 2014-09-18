package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.javatuples.Pair;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedEdgeTest extends AbstractGremlinTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullElement() {
        DetachedEdge.detach(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithSomethingAlreadyDetached() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v);
        DetachedEdge.detach(DetachedEdge.detach(e));
    }

    @Test
    @Ignore
    public void shouldConstructDetachedEdge() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("knows", v);
        final DetachedEdge detachedEdge = DetachedEdge.detach(e);
        assertEquals("3", detachedEdge.id());
        assertEquals("knows", detachedEdge.label());
        assertEquals(DetachedVertex.class, detachedEdge.iterators().vertices(Direction.OUT).next().getClass());
        assertEquals("1", detachedEdge.iterators().vertices(Direction.OUT).next().id());
        assertEquals(DetachedVertex.class, detachedEdge.iterators().vertices(Direction.IN).next().getClass());
        assertEquals("2", detachedEdge.iterators().vertices(Direction.IN).next().id());

        assertEquals(1, StreamFactory.stream(detachedEdge.iterators()).count());
        assertEquals("a", detachedEdge.iterators().properties("x").next().value());
        assertEquals(1, StreamFactory.stream(detachedEdge.iterators().properties("x")).count());
        assertEquals("b", detachedEdge.iterators().hiddens("y").next().value());
        assertEquals(1, StreamFactory.stream(detachedEdge.iterators().hiddens("y")).count());
    }

    @Test
    @Ignore
    public void shouldEvaluateToEqual() {
        //final DetachedEdge detachedEdge1 = DetachedEdge.detach(e);
        //assertTrue(detachedEdge1.equals(this.detachedEdge));
    }

    @Test
    @Ignore
    public void shouldNotEvaluateToEqualDifferentId() {
        //final DetachedEdge detachedEdge1 = DetachedEdge.detach(e);
        //assertFalse(detachedEdge1.equals(this.detachedEdge));
    }

    @Test
    public void shouldConstructDetachedEdgeFromParts() {
        final Map<String,Object> properties = new HashMap<>();
        properties.put("x", "a");

        final Map<String,Object> hiddens = new HashMap<>();
        hiddens.put(Graph.Key.hide("y"), "b");

        final DetachedEdge de = new DetachedEdge(10, "bought", properties, hiddens, Pair.with(1, "person"), Pair.with(2, "product"));

        assertEquals(10, de.id());
        assertEquals("bought", de.label());
        assertEquals("person", de.iterators().vertices(Direction.OUT).next().label());
        assertEquals(1, de.iterators().vertices(Direction.OUT).next().id());
        assertEquals("product", de.iterators().vertices(Direction.IN).next().label());
        assertEquals(2, de.iterators().vertices(Direction.IN).next().id());

        assertEquals(1, StreamFactory.stream(de.iterators()).count());
        assertEquals("a", de.iterators().properties("x").next().value());
        assertEquals(1, StreamFactory.stream(de.iterators().properties("x")).count());
        assertEquals("b", de.iterators().hiddens("y").next().value());
        assertEquals(1, StreamFactory.stream(de.iterators().hiddens("y")).count());

        assertEquals("a", de.property("x").value());
        assertEquals("x", de.property("x").key());
        assertFalse(de.property("x").isHidden());

        assertEquals("b", de.property(Graph.Key.hide("y")).value());
        assertEquals("y", de.property(Graph.Key.hide("y")).key());
        assertTrue(de.property(Graph.Key.hide("y")).isHidden());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowSetProperty() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v);
        final DetachedEdge detachedEdge = DetachedEdge.detach(e);
        detachedEdge.property("test", "test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowRemove() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v);
        final DetachedEdge detachedEdge = DetachedEdge.detach(e);
        detachedEdge.remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotTraverse() {
        final Vertex v = g.addVertex();
        final Edge e = v.addEdge("test", v);
        final DetachedEdge detachedEdge = DetachedEdge.detach(e);
        detachedEdge.start();
    }
}
