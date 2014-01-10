package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.AbstractBlueprintsTest;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IdGraphStrategyTest extends AbstractBlueprintsTest {
    private static final String idKey = "myId";

    public IdGraphStrategyTest() {
        super(Optional.of(new IdGraphStrategy(idKey)));
    }

    @Test
    public void shouldInjectAnIdAndReturnBySpecifiedId() {
        final Vertex v = g.addVertex(Property.Key.ID, "test", "something", "else");

        assertNotNull(v);
        assertEquals("test", v.getId());
        assertEquals("test", v.getProperty(Property.Key.hidden(idKey)).get());
        assertEquals("else", v.getProperty("something").get());

        final Vertex found = g.query().ids("test").vertices().iterator().next();
        assertEquals(v, found);
    }
}
