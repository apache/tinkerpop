package com.tinkerpop.gremlin.structure.util.micro;

import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class MicroVertexTest {

    private MicroVertex mv;

    @Before
    public void setup() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("1");
        when(v.label()).thenReturn("l");

        this.mv = MicroVertex.deflate(v);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullElement() {
        MicroVertex.deflate(null);
    }

    @Test
    public void shouldConstructMicroVertex() {
        assertEquals("1", this.mv.id());
        assertEquals("l", this.mv.label());
    }

    @Test
    public void shouldEvaluateToEqual() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("1");
        when(v.label()).thenReturn("l");

        final MicroVertex mv1 = MicroVertex.deflate(v);
        assertTrue(mv1.equals(this.mv));
    }

    @Test
    public void shouldNotEvaluateToEqualDifferentId() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("2");
        when(v.label()).thenReturn("l");

        final MicroVertex mv1 = MicroVertex.deflate(v);
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
    public void shouldNotAllowGetProperty() {
        this.mv.property("test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowRemove() {
        this.mv.remove();
    }
}
