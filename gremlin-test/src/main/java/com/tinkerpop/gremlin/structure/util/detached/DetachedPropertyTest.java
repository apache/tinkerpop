package com.tinkerpop.gremlin.structure.util.detached;

import com.tinkerpop.gremlin.AbstractGremlinTest;
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
public class DetachedPropertyTest extends AbstractGremlinTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotConstructWithNullProperty() {
        DetachedProperty.detach(null);
    }

    @Test(expected = IllegalArgumentException.class)
    @org.junit.Ignore
    public void shouldNotConstructWithSomethingAlreadyDetached() {
        //DetachedProperty.detach(this.mp);
    }

    @Test
    @org.junit.Ignore
    public void shouldConstructDetachedPropertyWithPropertyFromEdge() {
        //final DetachedProperty mp = DetachedProperty.detach(p);
        //assertEquals("k", mp.key());
        //assertEquals("val", mp.value());
        //assertEquals(DetachedEdge.class, mp.getElement().getClass());
    }

    @Test(expected = UnsupportedOperationException.class)
    @org.junit.Ignore
    public void shouldNotSupportRemove() {
        //this.mp.remove();
    }

    @Test
    @org.junit.Ignore
    public void shouldBeEqualsProperties() {
       //assertTrue(mp2.equals(this.mp));
    }

    @Test
    @org.junit.Ignore
    public void shouldNotBeEqualsPropertiesAsThereIsDifferentElement() {
        //assertFalse(mp2.equals(this.mp));
    }

    @Test
    @org.junit.Ignore
    public void shouldNotBeEqualsPropertiesAsThereIsDifferentKey() {
        //assertFalse(mp2.equals(this.mp));
    }

    @Test
    @org.junit.Ignore
    public void shouldNotBeEqualsPropertiesAsThereIsDifferentValue() {
        //assertFalse(mp2.equals(this.mp));
    }
}
