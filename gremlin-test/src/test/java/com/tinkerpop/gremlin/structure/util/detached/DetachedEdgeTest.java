package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.javatuples.Pair;
import org.junit.Before;
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
public class DetachedEdgeTest {

    private DetachedEdge detachedEdge;

    @Before
    public void setup() {
        final Vertex v1 = mock(Vertex.class);
        when(v1.id()).thenReturn("1");
        when(v1.label()).thenReturn("l");
        final Vertex v2 = mock(Vertex.class);
        when(v2.id()).thenReturn("2");
        when(v2.label()).thenReturn("l");

        final Edge e = mock(Edge.class);
        final Edge.Iterators edgeIterators = mock(Edge.Iterators.class);
        when(edgeIterators.vertices(Direction.OUT)).thenReturn(Arrays.asList(v1).iterator());
        when(edgeIterators.vertices(Direction.IN)).thenReturn(Arrays.asList(v2).iterator());

        final Property p1 = mock(Property.class);
        when(p1.key()).thenReturn("x");
        when(p1.isHidden()).thenReturn(false);
        when(p1.value()).thenReturn("a");
        when(p1.getElement()).thenReturn(e);
        final Property h1 = mock(Property.class);
        when(h1.key()).thenReturn(Graph.Key.hide("y"));
        when(h1.isHidden()).thenReturn(true);
        when(h1.value()).thenReturn("b");
        when(h1.getElement()).thenReturn(e);

        when(edgeIterators.hiddens()).thenReturn((Iterator) Arrays.asList(h1).iterator());
        when(edgeIterators.properties()).thenReturn((Iterator) Arrays.asList(p1).iterator());
        when(e.id()).thenReturn("3");
        when(e.label()).thenReturn("knows");
        when(e.iterators()).thenReturn(edgeIterators);

        this.detachedEdge = DetachedEdge.detach(e);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullElement() {
        DetachedEdge.detach(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithSomethingAlreadyDetached() {
        DetachedEdge.detach(this.detachedEdge);
    }

    @Test
    public void shouldConstructDetachedEdge() {
        assertEquals("3", this.detachedEdge.id());
        assertEquals("knows", this.detachedEdge.label());
        assertEquals(DetachedVertex.class, this.detachedEdge.iterators().vertices(Direction.OUT).next().getClass());
        assertEquals("1", this.detachedEdge.iterators().vertices(Direction.OUT).next().id());
        assertEquals(DetachedVertex.class, this.detachedEdge.iterators().vertices(Direction.IN).next().getClass());
        assertEquals("2", this.detachedEdge.iterators().vertices(Direction.IN).next().id());

        assertEquals(1, StreamFactory.stream(this.detachedEdge.iterators()).count());
        assertEquals("a", this.detachedEdge.iterators().properties("x").next().value());
        assertEquals(1, StreamFactory.stream(this.detachedEdge.iterators().properties("x")).count());
        assertEquals("b", this.detachedEdge.iterators().hiddens("y").next().value());
        assertEquals(1, StreamFactory.stream(this.detachedEdge.iterators().hiddens("y")).count());
    }

    @Test
    public void shouldEvaluateToEqual() {
        final Vertex v1 = mock(Vertex.class);
        when(v1.id()).thenReturn("1");
        when(v1.label()).thenReturn("l");
        final Vertex v2 = mock(Vertex.class);
        when(v2.id()).thenReturn("2");
        when(v2.label()).thenReturn("l");

        final Edge e = mock(Edge.class);
        when(e.id()).thenReturn("3");
        when(e.label()).thenReturn("knows");
        final Edge.Iterators edgeIterators = mock(Edge.Iterators.class);
        when(edgeIterators.vertices(Direction.OUT)).thenReturn(Arrays.asList(v1).iterator());
        when(edgeIterators.vertices(Direction.IN)).thenReturn(Arrays.asList(v2).iterator());
        when(edgeIterators.properties()).thenReturn(Collections.emptyIterator());
        when(edgeIterators.hiddens()).thenReturn(Collections.emptyIterator());
        when(e.iterators()).thenReturn(edgeIterators);

        final DetachedEdge detachedEdge1 = DetachedEdge.detach(e);
        assertTrue(detachedEdge1.equals(this.detachedEdge));
    }

    @Test
    public void shouldNotEvaluateToEqualDifferentId() {
        final Vertex v1 = mock(Vertex.class);
        when(v1.id()).thenReturn("1");
        when(v1.label()).thenReturn("l");
        final Vertex v2 = mock(Vertex.class);
        when(v2.id()).thenReturn("2");
        when(v2.label()).thenReturn("l");

        final Edge e = mock(Edge.class);
        when(e.id()).thenReturn("4");
        when(e.label()).thenReturn("knows");
        final Edge.Iterators edgeIterators = mock(Edge.Iterators.class);
        when(edgeIterators.vertices(Direction.OUT)).thenReturn(Arrays.asList(v1).iterator());
        when(edgeIterators.vertices(Direction.IN)).thenReturn(Arrays.asList(v2).iterator());
        when(edgeIterators.properties()).thenReturn(Collections.emptyIterator());
        when(edgeIterators.hiddens()).thenReturn(Collections.emptyIterator());
        when(e.iterators()).thenReturn(edgeIterators);

        final DetachedEdge detachedEdge1 = DetachedEdge.detach(e);
        assertFalse(detachedEdge1.equals(this.detachedEdge));
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
        this.detachedEdge.property("test", "test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowRemove() {
        this.detachedEdge.remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotTraverse() {
        this.detachedEdge.start();
    }
}
