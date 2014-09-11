package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedMetaPropertyTest {

    private DetachedMetaProperty mp;

    @Before
    public void setup() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("1");
        when(v.label()).thenReturn("person");

        final MetaProperty p = mock(MetaProperty.class);
        when(p.id()).thenReturn(1);
        when(p.label()).thenReturn(MetaProperty.DEFAULT_LABEL);
        when(p.key()).thenReturn("k");
        when(p.getElement()).thenReturn(v);
        when(p.value()).thenReturn("val");

        this.mp = DetachedMetaProperty.detach(p);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullProperty() {
        DetachedProperty.detach(null);
    }

    @Test
    public void shouldConstructDetachedPropertyWithPropertyFromVertex() {
        assertEquals("k", mp.key());
        assertEquals("val", mp.value());
        assertEquals(DetachedVertex.class, mp.getElement().getClass());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotSupportRemove() {
        this.mp.remove();
    }

    @Test
    public void shouldBeEqualsPropertiesAsIdIsTheSame() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("1");
        when(v.label()).thenReturn("person");
        final MetaProperty p = mock(MetaProperty.class);
        when(p.id()).thenReturn(1);
        when(p.label()).thenReturn(MetaProperty.DEFAULT_LABEL);
        when(p.key()).thenReturn("k");
        when(p.getElement()).thenReturn(v);
        when(p.value()).thenReturn("val");

        final DetachedMetaProperty mp2 = DetachedMetaProperty.detach(p);

        assertTrue(mp2.equals(this.mp));
    }

    @Test
    public void shouldBeEqualsSinceIdIsSameEvenThoughPropertiesHaveDifferentVertex() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("2");
        when(v.label()).thenReturn("person");
        final MetaProperty p = mock(MetaProperty.class);
        when(p.id()).thenReturn(1);
        when(p.label()).thenReturn(MetaProperty.DEFAULT_LABEL);
        when(p.key()).thenReturn("k");
        when(p.getElement()).thenReturn(v);
        when(p.value()).thenReturn("val");

        final DetachedMetaProperty mp2 = DetachedMetaProperty.detach(p);

        assertTrue(mp2.equals(this.mp));
    }

    @Test
    public void shouldBeEqualsSinceIdIsSameEvenThoughPropertiesHaveDifferentKeys() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("1");
        when(v.label()).thenReturn("person");
        final MetaProperty p = mock(MetaProperty.class);
        when(p.id()).thenReturn(1);
        when(p.label()).thenReturn(MetaProperty.DEFAULT_LABEL);
        when(p.key()).thenReturn("k1");
        when(p.getElement()).thenReturn(v);
        when(p.value()).thenReturn("val");

        final DetachedMetaProperty mp2 = DetachedMetaProperty.detach(p);

        assertTrue(mp2.equals(this.mp));
    }

    @Test
    public void shouldBeEqualsSinceIdIsSameEvenThoughPropertiesHaveDifferentValues() {
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn("1");
        when(v.label()).thenReturn("person");
        final MetaProperty p = mock(MetaProperty.class);
        when(p.id()).thenReturn(1);
        when(p.label()).thenReturn(MetaProperty.DEFAULT_LABEL);
        when(p.key()).thenReturn("k");
        when(p.getElement()).thenReturn(v);
        when(p.value()).thenReturn("val1");

        final DetachedMetaProperty mp2 = DetachedMetaProperty.detach(p);

        assertTrue(mp2.equals(this.mp));
    }
}
