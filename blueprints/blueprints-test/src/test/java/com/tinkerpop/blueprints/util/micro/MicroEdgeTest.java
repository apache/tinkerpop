package com.tinkerpop.blueprints.util.micro;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
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
public class MicroEdgeTest {

    private MicroEdge me;

    @Before
    public void setup() {
        final Vertex v1 = mock(Vertex.class);
        when(v1.getId()).thenReturn("1");
        when(v1.getLabel()).thenReturn("l");
        final Vertex v2 = mock(Vertex.class);
        when(v2.getId()).thenReturn("2");
        when(v2.getLabel()).thenReturn("l");

        final Edge e = mock(Edge.class);
        when(e.getId()).thenReturn("3");
        when(e.getLabel()).thenReturn("knows");
        when(e.getVertex(Direction.OUT)).thenReturn(v1);
        when(e.getVertex(Direction.IN)).thenReturn(v2);

        this.me = new MicroEdge(e);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullElement() {
        new MicroEdge(null);
    }

    @Test
    public void shouldConstructMicroEdge() {
        assertEquals("3", this.me.getId());
        assertEquals("knows", this.me.getLabel());
        assertEquals(MicroVertex.class, this.me.getVertex(Direction.OUT).getClass());
        assertEquals("1", this.me.getVertex(Direction.OUT).getId());
        assertEquals(MicroVertex.class, this.me.getVertex(Direction.IN).getClass());
        assertEquals("2", this.me.getVertex(Direction.IN).getId());
    }

    @Test
    public void shouldEvaluateToEqual() {
        final Vertex v1 = mock(Vertex.class);
        when(v1.getId()).thenReturn("1");
        when(v1.getLabel()).thenReturn("l");
        final Vertex v2 = mock(Vertex.class);
        when(v2.getId()).thenReturn("2");
        when(v2.getLabel()).thenReturn("l");

        final Edge e = mock(Edge.class);
        when(e.getId()).thenReturn("3");
        when(e.getLabel()).thenReturn("knows");
        when(e.getVertex(Direction.OUT)).thenReturn(v1);
        when(e.getVertex(Direction.IN)).thenReturn(v2);

        final MicroEdge me1 = new MicroEdge(e);
        assertTrue(me1.equals(this.me));
    }

    @Test
    public void shouldNotEvaluateToEqualDifferentId() {
        final Vertex v1 = mock(Vertex.class);
        when(v1.getId()).thenReturn("1");
        when(v1.getLabel()).thenReturn("l");
        final Vertex v2 = mock(Vertex.class);
        when(v2.getId()).thenReturn("2");
        when(v2.getLabel()).thenReturn("l");

        final Edge e = mock(Edge.class);
        when(e.getId()).thenReturn("4");
        when(e.getLabel()).thenReturn("knows");
        when(e.getVertex(Direction.OUT)).thenReturn(v1);
        when(e.getVertex(Direction.IN)).thenReturn(v2);

        final MicroEdge me1 = new MicroEdge(e);
        assertFalse(me1.equals(this.me));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowSetProperty() {
        this.me.setProperty("test", "test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowGetProperty() {
        this.me.getProperty("test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotAllowRemove() {
        this.me.remove();
    }
}
