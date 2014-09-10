package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.SingleGraphTraversal;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedEdgeTest {

    private DetachedEdge me;

    @Before
    public void setup() {
        final Vertex v1 = mock(Vertex.class);
        when(v1.id()).thenReturn("1");
        when(v1.label()).thenReturn("l");
        final Vertex v2 = mock(Vertex.class);
        when(v2.id()).thenReturn("2");
        when(v2.label()).thenReturn("l");

        final Edge e = mock(Edge.class);
        when(e.id()).thenReturn("3");
        when(e.label()).thenReturn("knows");
        when(e.outV()).thenReturn(new SingleGraphTraversal(v1));
        when(e.inV()).thenReturn(new SingleGraphTraversal(v2));

        this.me = DetachedEdge.detach(e);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullElement() {
        DetachedEdge.detach(null);
    }

    @Test
    public void shouldConstructDetachedEdge() {
        assertEquals("3", this.me.id());
        assertEquals("knows", this.me.label());
        assertEquals(DetachedVertex.class, this.me.outV().next().getClass());
        assertEquals("1", this.me.outV().id().next());
        assertEquals(DetachedVertex.class, this.me.inV().next().getClass());
        assertEquals("2", this.me.inV().id().next());
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
        when(e.outV()).thenReturn(new SingleGraphTraversal(v1));
        when(e.inV()).thenReturn(new SingleGraphTraversal(v2));

        final DetachedEdge me1 = DetachedEdge.detach(e);
        assertTrue(me1.equals(this.me));
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
        when(e.outV()).thenReturn(new SingleGraphTraversal(v1));
        when(e.inV()).thenReturn(new SingleGraphTraversal(v2));

        final DetachedEdge me1 = DetachedEdge.detach(e);
        assertFalse(me1.equals(this.me));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowSetProperty() {
        this.me.property("test", "test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowRemove() {
        this.me.remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotTraverse() {
        this.me.start();
    }
}
