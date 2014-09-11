package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedPropertyTest {

    private DetachedProperty mp;

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
        when(e.id()).thenReturn("1");
        when(e.label()).thenReturn("knows");
        when(e.iterators()).thenReturn(edgeIterators);
        when(edgeIterators.hiddens()).thenReturn(Collections.emptyIterator());
        when(edgeIterators.properties()).thenReturn(Collections.emptyIterator());

        final Property p = mock(Property.class);
        when(p.key()).thenReturn("k");
        when(p.getElement()).thenReturn(e);
        when(p.value()).thenReturn("val");

        this.mp = DetachedProperty.detach(p);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullProperty() {
        DetachedProperty.detach(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithSomethingAlreadyDetached() {
        DetachedProperty.detach(this.mp);
    }

    @Test
    public void shouldConstructDetachedPropertyWithPropertyFromEdge() {
        final Vertex v1 = mock(Vertex.class);
        final Vertex v2 = mock(Vertex.class);
        final Edge e = mock(Edge.class);
        when(e.id()).thenReturn("14");
        when(e.label()).thenReturn("knows");
        final Edge.Iterators edgeIterators = mock(Edge.Iterators.class);
        when(edgeIterators.vertices(Direction.OUT)).thenReturn(Arrays.asList(v1).iterator());
        when(edgeIterators.vertices(Direction.IN)).thenReturn(Arrays.asList(v2).iterator());
        when(edgeIterators.properties()).thenReturn(Collections.emptyIterator());
        when(edgeIterators.hiddens()).thenReturn(Collections.emptyIterator());
        when(e.iterators()).thenReturn(edgeIterators);

        when(v1.id()).thenReturn("1");
        when(v1.label()).thenReturn("person");
        when(v2.id()).thenReturn("2");
        when(v2.label()).thenReturn("person");

        final Property p = mock(Property.class);
        when(p.key()).thenReturn("k");
        when(p.getElement()).thenReturn(e);
        when(p.value()).thenReturn("val");

        final DetachedProperty mp = DetachedProperty.detach(p);
        assertEquals("k", mp.key());
        assertEquals("val", mp.value());
        assertEquals(DetachedEdge.class, mp.getElement().getClass());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotSupportRemove() {
        this.mp.remove();
    }

    @Test
    public void shouldBeEqualsProperties() {
        final Vertex v1 = mock(Vertex.class);
        final Vertex v2 = mock(Vertex.class);
        final Edge e = mock(Edge.class);
        when(e.id()).thenReturn("1");
        when(e.label()).thenReturn("person");
        final Edge.Iterators edgeIterators = mock(Edge.Iterators.class);
        when(edgeIterators.vertices(Direction.OUT)).thenReturn(Arrays.asList(v1).iterator());
        when(edgeIterators.vertices(Direction.IN)).thenReturn(Arrays.asList(v2).iterator());
        when(edgeIterators.properties()).thenReturn(Collections.emptyIterator());
        when(edgeIterators.hiddens()).thenReturn(Collections.emptyIterator());
        when(e.iterators()).thenReturn(edgeIterators);

        when(v1.id()).thenReturn("1");
        when(v1.label()).thenReturn("person");
        when(v2.id()).thenReturn("2");
        when(v2.label()).thenReturn("person");

        final Property p = mock(Property.class);
        when(p.key()).thenReturn("k");
        when(p.getElement()).thenReturn(e);
        when(p.value()).thenReturn("val");

        final DetachedProperty mp2 = DetachedProperty.detach(p);

        assertTrue(mp2.equals(this.mp));
    }

    @Test
    public void shouldNotBeEqualsPropertiesAsThereIsDifferentElement() {
        final Vertex v1 = mock(Vertex.class);
        final Vertex v2 = mock(Vertex.class);
        final Edge e = mock(Edge.class);
        when(e.id()).thenReturn("2");
        when(e.label()).thenReturn("person");
        final Edge.Iterators edgeIterators = mock(Edge.Iterators.class);
        when(edgeIterators.vertices(Direction.OUT)).thenReturn(Arrays.asList(v1).iterator());
        when(edgeIterators.vertices(Direction.IN)).thenReturn(Arrays.asList(v2).iterator());
        when(edgeIterators.properties()).thenReturn(Collections.emptyIterator());
        when(edgeIterators.hiddens()).thenReturn(Collections.emptyIterator());
        when(e.iterators()).thenReturn(edgeIterators);

        when(v1.id()).thenReturn("1");
        when(v1.label()).thenReturn("person");
        when(v2.id()).thenReturn("2");
        when(v2.label()).thenReturn("person");

        final Property p = mock(Property.class);
        when(p.key()).thenReturn("k");
        when(p.getElement()).thenReturn(e);
        when(p.value()).thenReturn("val");

        final DetachedProperty mp2 = DetachedProperty.detach(p);

        assertFalse(mp2.equals(this.mp));
    }

    @Test
    public void shouldNotBeEqualsPropertiesAsThereIsDifferentKey() {
        final Vertex v1 = mock(Vertex.class);
        final Vertex v2 = mock(Vertex.class);
        final Edge e = mock(Edge.class);
        when(e.id()).thenReturn("1");
        when(e.label()).thenReturn("person");
        final Edge.Iterators edgeIterators = mock(Edge.Iterators.class);
        when(edgeIterators.vertices(Direction.OUT)).thenReturn(Arrays.asList(v1).iterator());
        when(edgeIterators.vertices(Direction.IN)).thenReturn(Arrays.asList(v2).iterator());
        when(edgeIterators.properties()).thenReturn(Collections.emptyIterator());
        when(edgeIterators.hiddens()).thenReturn(Collections.emptyIterator());
        when(e.iterators()).thenReturn(edgeIterators);

        when(v1.id()).thenReturn("1");
        when(v1.label()).thenReturn("person");
        when(v2.id()).thenReturn("2");
        when(v2.label()).thenReturn("person");

        final Property p = mock(Property.class);
        when(p.key()).thenReturn("k1");
        when(p.getElement()).thenReturn(e);
        when(p.value()).thenReturn("val");

        final DetachedProperty mp2 = DetachedProperty.detach(p);

        assertFalse(mp2.equals(this.mp));
    }

    @Test
    public void shouldNotBeEqualsPropertiesAsThereIsDifferentValue() {
        final Vertex v1 = mock(Vertex.class);
        final Vertex v2 = mock(Vertex.class);
        final Edge e = mock(Edge.class);
        when(e.id()).thenReturn("1");
        when(e.label()).thenReturn("person");
        final Edge.Iterators edgeIterators = mock(Edge.Iterators.class);
        when(edgeIterators.vertices(Direction.OUT)).thenReturn(Arrays.asList(v1).iterator());
        when(edgeIterators.vertices(Direction.IN)).thenReturn(Arrays.asList(v2).iterator());
        when(edgeIterators.properties()).thenReturn(Collections.emptyIterator());
        when(edgeIterators.hiddens()).thenReturn(Collections.emptyIterator());
        when(e.iterators()).thenReturn(edgeIterators);

        when(v1.id()).thenReturn("1");
        when(v1.label()).thenReturn("person");
        when(v2.id()).thenReturn("2");
        when(v2.label()).thenReturn("person");

        final Property p = mock(Property.class);
        when(p.key()).thenReturn("k");
        when(p.getElement()).thenReturn(e);
        when(p.value()).thenReturn("val1");

        final DetachedProperty mp2 = DetachedProperty.detach(p);

        assertFalse(mp2.equals(this.mp));
    }
}
