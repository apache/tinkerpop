package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedVertexTest {

    private DetachedVertex detachedVertex;

    @Before
    public void setup() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("1");
        when(v.label()).thenReturn("l");

        this.detachedVertex = DetachedVertex.detach(v);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullElement() {
        DetachedVertex.detach(null);
    }

    @Test
    public void shouldConstructDetachedVertex() {
        assertEquals("1", this.detachedVertex.id());
        assertEquals("l", this.detachedVertex.label());
    }

    @Test
    public void shouldEvaluateToEqual() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("1");
        when(v.label()).thenReturn("l");

        final DetachedVertex detachedVertex1 = DetachedVertex.detach(v);
        assertTrue(detachedVertex1.equals(this.detachedVertex));
    }

    @Test
    public void shouldNotEvaluateToEqualDifferentId() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("2");
        when(v.label()).thenReturn("l");

        final DetachedVertex detachedVertex1 = DetachedVertex.detach(v);
        assertFalse(detachedVertex1.equals(this.detachedVertex));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowAddEdge() {
        this.detachedVertex.addEdge("test", null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowSetProperty() {
        this.detachedVertex.property("test", "test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowRemove() {
        this.detachedVertex.remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotTraverse() {
        this.detachedVertex.out().out();
    }
}
