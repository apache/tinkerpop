package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MetaPropertyTest extends AbstractGremlinTest {

    @Test
    public void shouldAllowMultiProperties() {
        Vertex v = g.addVertex("name", "marko", "age", 34);
        assertEquals("marko", v.property("name").value());
        assertEquals("marko", v.value("name"));
        assertEquals(34, v.property("age").value());
        assertEquals(34, v.<Integer>value("age").intValue());
        assertEquals(1, v.properties("name").count().next().intValue());
        assertEquals(2, v.properties().count().next().intValue());

        v.property("name", "marko a. rodriguez");
        try {
            v.property("name");
            fail("This should throw a: " + Vertex.Exceptions.multiplePropertiesExistForProvidedKey("name"));
        } catch (final IllegalStateException e) {
            assertEquals(Vertex.Exceptions.multiplePropertiesExistForProvidedKey("name").getMessage(), e.getMessage());
        } catch (final Exception e) {
            fail("This should throw a: " + Vertex.Exceptions.multiplePropertiesExistForProvidedKey("name"));
        }
        assertEquals(2, v.properties("name").count().next().intValue());
        assertEquals(3, v.properties().count().next().intValue());
        assertTrue(v.valueMap().next().get("name") instanceof List);
        assertTrue(((List) v.valueMap().next().get("name")).contains("marko"));
        assertTrue(((List) v.valueMap().next().get("name")).contains("mrodriguez"));

        v.property("name", "mrodriguez");
        assertEquals(3, v.properties("name").count().next().intValue());
        assertEquals(4, v.properties().count().next().intValue());

        v.properties("name").sideEffect(p -> p.get().<Integer>property("counter", ((String) p.get().value()).length())).iterate();
        v.properties().forEach(p -> {
            assertTrue(p instanceof MetaProperty);
            if (p.key().equals("age")) {
                assertEquals(p.value(), 34);
                // assertEquals(0, p.properties());
            }
            if (p.key().equals("name")) {
                assertEquals(((String) p.value()).length(), p.<Integer>value("counter").intValue());
                // assertEquals(1, p.properties());
            }
        });

    }
}
