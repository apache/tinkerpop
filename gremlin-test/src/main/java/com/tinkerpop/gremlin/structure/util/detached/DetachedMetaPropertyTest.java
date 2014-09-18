package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.AbstractGremlinTest;
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
public class DetachedMetaPropertyTest extends AbstractGremlinTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullProperty() {
        DetachedMetaProperty.detach(null);
    }

    @Test(expected = IllegalArgumentException.class)
    @org.junit.Ignore
    public void shouldNotConstructWithSomethingAlreadyDetached() {
        //DetachedMetaProperty.detach(this.mp);
    }

    @Test
    @org.junit.Ignore
    public void shouldConstructDetachedPropertyWithPropertyFromVertex() {
        /*
        assertEquals("k", mp.key());
        assertEquals("val", mp.value());
        assertEquals(DetachedVertex.class, mp.getElement().getClass());
        */
    }

    @Test(expected = UnsupportedOperationException.class)
    @org.junit.Ignore
    public void shouldNotSupportRemove() {
        //this.mp.remove();
    }

    @Test
    @org.junit.Ignore
    public void shouldBeEqualsPropertiesAsIdIsTheSame() {
        //assertTrue(mp2.equals(this.mp));
    }

    @Test
    @org.junit.Ignore
    public void shouldBeEqualsSinceIdIsSameEvenThoughPropertiesHaveDifferentVertex() {
        //assertTrue(mp2.equals(this.mp));
    }

    @Test
    @org.junit.Ignore
    public void shouldBeEqualsSinceIdIsSameEvenThoughPropertiesHaveDifferentKeys() {
        //assertTrue(mp2.equals(this.mp));
    }

    @Test
    @org.junit.Ignore
    public void shouldBeEqualsSinceIdIsSameEvenThoughPropertiesHaveDifferentValues() {
        //assertTrue(mp2.equals(this.mp));
    }
}
