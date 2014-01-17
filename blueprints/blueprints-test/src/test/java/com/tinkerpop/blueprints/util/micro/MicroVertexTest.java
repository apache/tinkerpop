package com.tinkerpop.blueprints.util.micro;

import com.tinkerpop.blueprints.Vertex;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
        when(v.getId()).thenReturn("1");
        when(v.getLabel()).thenReturn("l");

        this.mv = new MicroVertex(v);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullElement() {
        new MicroVertex(null);
    }

    @Test
    public void shouldConstructMicroVertex() {
        assertEquals("1", this.mv.getId());
        assertEquals("l", this.mv.getLabel());
    }

    @Test
    public void shouldEvaluateToEqual() {
        final Vertex v = mock(Vertex.class);
        when(v.getId()).thenReturn("1");
        when(v.getLabel()).thenReturn("l");

        final MicroVertex mv1 = new MicroVertex(v);
        assertTrue(mv1.equals(this.mv));
    }

    @Test
    public void shouldNotEvaluateToEqualDifferentId() {
        final Vertex v = mock(Vertex.class);
        when(v.getId()).thenReturn("2");
        when(v.getLabel()).thenReturn("l");

        final MicroVertex mv1 = new MicroVertex(v);
        assertFalse(mv1.equals(this.mv));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowAddEdge() {
        this.mv.addEdge("test", null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowQuery() {
        this.mv.query();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowSetProperty() {
        this.mv.setProperty("test", "test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowGetProperty() {
        this.mv.getProperty("test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowRemove() {
        this.mv.remove();
    }
}
