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

    private DetachedVertex mv;

    @Before
    public void setup() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("1");
        when(v.label()).thenReturn("l");

        this.mv = DetachedVertex.detach(v);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullElement() {
        DetachedVertex.detach(null);
    }

    @Test
    public void shouldConstructDetachedVertex() {
        assertEquals("1", this.mv.id());
        assertEquals("l", this.mv.label());
    }

    @Test
    public void shouldEvaluateToEqual() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("1");
        when(v.label()).thenReturn("l");

        final DetachedVertex mv1 = DetachedVertex.detach(v);
        assertTrue(mv1.equals(this.mv));
    }

    @Test
    public void shouldNotEvaluateToEqualDifferentId() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("2");
        when(v.label()).thenReturn("l");

        final DetachedVertex mv1 = DetachedVertex.detach(v);
        assertFalse(mv1.equals(this.mv));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowAddEdge() {
        this.mv.addEdge("test", null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowSetProperty() {
        this.mv.property("test", "test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowRemove() {
        this.mv.remove();
    }
}
