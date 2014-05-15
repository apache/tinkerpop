package com.tinkerpop.gremlin.structure.util.micro;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
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
public class MicroPropertyTest {

    private MicroProperty mp;

    @Before
    public void setup() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("1");

        final Property p = mock(Property.class);
        when(p.getKey()).thenReturn("k");
        when(p.getElement()).thenReturn(v);
        when(p.get()).thenReturn("val");

        this.mp = MicroProperty.deflate(p);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullProperty() {
        MicroProperty.deflate(null);
    }

    @Test
    public void shouldConstructMicroPropertyWithPropertyFromVertex() {
        assertEquals("k", mp.getKey());
        assertEquals("val", mp.get());
        assertEquals(MicroVertex.class, mp.getElement().getClass());
    }

    @Test
    public void shouldConstructMicroPropertyWithPropertyFromEdge() {
        final Vertex v1 = mock(Vertex.class);
        final Vertex v2 = mock(Vertex.class);
        final Edge e = mock(Edge.class);
        when(e.outV()).thenReturn(new SingleGraphTraversal(v1));
        when(e.inV()).thenReturn(new SingleGraphTraversal(v2));

        final Property p = mock(Property.class);
        when(p.getKey()).thenReturn("k");
        when(p.getElement()).thenReturn(e);
        when(p.get()).thenReturn("val");

        final MicroProperty mp = MicroProperty.deflate(p);
        assertEquals("k", mp.getKey());
        assertEquals("val", mp.get());
        assertEquals(MicroEdge.class, mp.getElement().getClass());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotSupportRemove() {
        this.mp.remove();
    }

    @Test
    public void shouldBeEqualsProperties() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("1");
        final Property p = mock(Property.class);
        when(p.getKey()).thenReturn("k");
        when(p.getElement()).thenReturn(v);
        when(p.get()).thenReturn("val");

        final MicroProperty mp2 = MicroProperty.deflate(p);

        assertTrue(mp2.equals(this.mp));
    }

    @Test
    public void shouldNotBeEqualsPropertiesAsThereIsDifferentElement() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("2");
        final Property p = mock(Property.class);
        when(p.getKey()).thenReturn("k");
        when(p.getElement()).thenReturn(v);
        when(p.get()).thenReturn("val");

        final MicroProperty mp2 = MicroProperty.deflate(p);

        assertFalse(mp2.equals(this.mp));
    }

    @Test
    public void shouldNotBeEqualsPropertiesAsThereIsDifferentKey() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("1");
        final Property p = mock(Property.class);
        when(p.getKey()).thenReturn("k1");
        when(p.getElement()).thenReturn(v);
        when(p.get()).thenReturn("val");

        final MicroProperty mp2 = MicroProperty.deflate(p);

        assertFalse(mp2.equals(this.mp));
    }

    @Test
    public void shouldNotBeEqualsPropertiesAsThereIsDifferentValue() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("1");
        final Property p = mock(Property.class);
        when(p.getKey()).thenReturn("k");
        when(p.getElement()).thenReturn(v);
        when(p.get()).thenReturn("val1");

        final MicroProperty mp2 = MicroProperty.deflate(p);

        assertFalse(mp2.equals(this.mp));
    }
}
