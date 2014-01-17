package com.tinkerpop.blueprints.util.micro;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
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
public class MicroPropertyTest {

    private MicroProperty mp;

    @Before
    public void setup() {
        final Vertex v = mock(Vertex.class);
        when(v.getId()).thenReturn("1");

        final Property p = mock(Property.class);
        when(p.getKey()).thenReturn("k");
        when(p.getElement()).thenReturn(v);
        when(p.get()).thenReturn("val");

        this.mp = new MicroProperty(p);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullProperty() {
        new MicroProperty(null);
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
        when(e.getVertex(Direction.OUT)).thenReturn(v1);
        when(e.getVertex(Direction.IN)).thenReturn(v2);

        final Property p = mock(Property.class);
        when(p.getKey()).thenReturn("k");
        when(p.getElement()).thenReturn(e);
        when(p.get()).thenReturn("val");

        final MicroProperty mp = new MicroProperty(p);
        assertEquals("k", mp.getKey());
        assertEquals("val", mp.get());
        assertEquals(MicroEdge.class, mp.getElement().getClass());
    }

    @Test(expected=UnsupportedOperationException.class)
    public void shouldNotSupportRemove() {
        this.mp.remove();
    }

    @Test
    public void shouldBeEqualsProperties() {
        final Vertex v = mock(Vertex.class);
        when(v.getId()).thenReturn("1");
        final Property p = mock(Property.class);
        when(p.getKey()).thenReturn("k");
        when(p.getElement()).thenReturn(v);
        when(p.get()).thenReturn("val");

        final MicroProperty mp2 = new MicroProperty(p);

        assertTrue(mp2.equals(this.mp));
    }

    @Test
    public void shouldNotBeEqualsPropertiesAsThereIsDifferentElement() {
        final Vertex v = mock(Vertex.class);
        when(v.getId()).thenReturn("2");
        final Property p = mock(Property.class);
        when(p.getKey()).thenReturn("k");
        when(p.getElement()).thenReturn(v);
        when(p.get()).thenReturn("val");

        final MicroProperty mp2 = new MicroProperty(p);

        assertFalse(mp2.equals(this.mp));
    }

    @Test
    public void shouldNotBeEqualsPropertiesAsThereIsDifferentKey() {
        final Vertex v = mock(Vertex.class);
        when(v.getId()).thenReturn("1");
        final Property p = mock(Property.class);
        when(p.getKey()).thenReturn("k1");
        when(p.getElement()).thenReturn(v);
        when(p.get()).thenReturn("val");

        final MicroProperty mp2 = new MicroProperty(p);

        assertFalse(mp2.equals(this.mp));
    }

    @Test
    public void shouldNotBeEqualsPropertiesAsThereIsDifferentValue() {
        final Vertex v = mock(Vertex.class);
        when(v.getId()).thenReturn("1");
        final Property p = mock(Property.class);
        when(p.getKey()).thenReturn("k");
        when(p.getElement()).thenReturn(v);
        when(p.get()).thenReturn("val1");

        final MicroProperty mp2 = new MicroProperty(p);

        assertFalse(mp2.equals(this.mp));
    }
}
