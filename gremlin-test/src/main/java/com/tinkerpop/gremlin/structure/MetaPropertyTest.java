package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MetaPropertyTest extends AbstractGremlinTest {

    @Test
    public void shouldAddMultiProperties() {
        Vertex v = g.addVertex("name", "marko", "age", 34);
        assertEquals("marko", v.property("name").value());
        assertEquals("marko", v.value("name"));
        assertEquals(34, v.property("age").value());
        assertEquals(34, v.<Integer>value("age").intValue());
        assertEquals(1, v.properties("name").count().next().intValue());
        assertEquals(2, v.properties().count().next().intValue());
        assertEquals(1, g.V().count().next().intValue());
        assertEquals(0, g.E().count().next().intValue());

        assertEquals(v, v.property("name", "marko a. rodriguez").getElement());
        try {
            v.property("name");
            fail("This should throw a: " + Vertex.Exceptions.multiplePropertiesExistForProvidedKey("name"));
        } catch (final IllegalStateException e) {
            assertEquals(Vertex.Exceptions.multiplePropertiesExistForProvidedKey("name").getMessage(), e.getMessage());
        } catch (final Exception e) {
            fail("This should throw a: " + Vertex.Exceptions.multiplePropertiesExistForProvidedKey("name"));
        }
        assertTrue(v.valueMap().next().get("name").contains("marko"));
        assertTrue(v.valueMap().next().get("name").contains("marko a. rodriguez"));
        assertEquals(3, v.properties().count().next().intValue());
        assertEquals(2, v.properties("name").count().next().intValue());
        assertEquals(1, g.V().count().next().intValue());
        assertEquals(0, g.E().count().next().intValue());

        assertEquals(v, v.property("name", "mrodriguez").getElement());
        assertEquals(3, v.properties("name").count().next().intValue());
        assertEquals(4, v.properties().count().next().intValue());
        assertEquals(1, g.V().count().next().intValue());
        assertEquals(0, g.E().count().next().intValue());

        v.properties("name").sideEffect(meta -> meta.get().<Integer>property("counter", ((String) meta.get().value()).length())).iterate();
        v.properties().forEach(meta -> {
            assertTrue(meta.isPresent());
            assertFalse(meta.isHidden());
            assertEquals(v, meta.getElement());
            if (meta.key().equals("age")) {
                assertEquals(meta.value(), 34);
                // assertEquals(0, p.properties());
            }
            if (meta.key().equals("name")) {
                assertEquals(((String) meta.value()).length(), meta.<Integer>value("counter").intValue());
                // assertEquals(1, p.properties());
            }
        });
        assertEquals(1, g.V().count().next().intValue());
        assertEquals(0, g.E().count().next().intValue());
    }

    @Test
    public void shouldRemoveMultiProperties() {
        Vertex v = g.addVertex("name", "marko", "age", 34);
        v.property("name", "marko a. rodriguez");
        v.property("name", "marko rodriguez");
        v.property("name", "marko");
        assertEquals(5, v.properties().count().next().intValue());
        assertEquals(4, v.properties().has(MetaProperty.KEY, "name").count().next().intValue());
        assertEquals(4, v.properties("name").count().next().intValue());
        assertEquals(1, v.properties("name").has(MetaProperty.VALUE, "marko a. rodriguez").count().next().intValue());
        assertEquals(1, v.properties("name").has(MetaProperty.VALUE, "marko rodriguez").count().next().intValue());
        assertEquals(2, v.properties("name").has(MetaProperty.VALUE, "marko").count().next().intValue());
        assertEquals(1, g.V().count().next().intValue());
        assertEquals(0, g.E().count().next().intValue());

        v.properties().has(MetaProperty.VALUE, "marko").remove();
        assertEquals(3, v.properties().count().next().intValue());
        assertEquals(2, v.properties().has(MetaProperty.KEY, "name").count().next().intValue());
        assertEquals(1, g.V().count().next().intValue());
        assertEquals(0, g.E().count().next().intValue());

        v.property("age").remove();
        assertEquals(2, v.properties().count().next().intValue());
        assertEquals(2, v.properties().has(MetaProperty.KEY, "name").count().next().intValue());
        assertEquals(1, g.V().count().next().intValue());
        assertEquals(0, g.E().count().next().intValue());

        v.properties("name").has(MetaProperty.KEY, "name").remove();
        assertEquals(0, v.properties().count().next().intValue());
        assertEquals(0, v.properties().has(MetaProperty.KEY, "name").count().next().intValue());
        assertEquals(1, g.V().count().next().intValue());
        assertEquals(0, g.E().count().next().intValue());
    }

    @Test
    public void shouldSupportPropertiesOnMultiProperties() {
        Vertex v = g.addVertex("name", "marko", "age", 34);
        assertEquals(2, g.V().properties().count().next().intValue());
        assertEquals(1, g.V().count().next().intValue());
        assertEquals(0, g.E().count().next().intValue());

        // TODO: Neo4j needs a better ID system for MetaProperties
        assertEquals(v.property("name"), v.property("name").property("acl", "public").getElement());
        assertEquals(v.property("age"), v.property("age").property("acl", "private").getElement());

        v.property("name").property("acl", "public");
        v.property("age").property("acl", "private");

        assertEquals(2, g.V().properties().count().next().intValue());
        assertEquals(1, g.V().properties("age").count().next().intValue());
        assertEquals(1, g.V().properties("name").count().next().intValue());
        assertEquals(1, g.V().properties("age").properties().count().next().intValue());
        assertEquals(1, g.V().properties("name").properties().count().next().intValue());
        assertEquals(1, g.V().properties("age").properties("acl").count().next().intValue());
        assertEquals(1, g.V().properties("name").properties("acl").count().next().intValue());
        assertEquals("private", g.V().properties("age").properties("acl").value().next());
        assertEquals("public", g.V().properties("name").properties("acl").value().next());
        assertEquals("private", g.V().properties("age").value("acl").next());
        assertEquals("public", g.V().properties("name").value("acl").next());
        assertEquals(1, g.V().count().next().intValue());
        assertEquals(0, g.E().count().next().intValue());

        v.property("age").property("acl", "public");
        v.property("age").property("changeDate", 2014);
        assertEquals("public", g.V().properties("age").value("acl").next());
        assertEquals(2014, g.V().properties("age").value("changeDate").next());
        assertEquals(1, v.properties("age").valueMap().count().next().intValue());
        assertEquals(2, v.properties("age").valueMap().next().size());
        assertTrue(v.properties("age").valueMap().next().containsKey("acl"));
        assertTrue(v.properties("age").valueMap().next().containsKey("changeDate"));
        assertEquals("public", v.properties("age").valueMap().next().get("acl"));
        assertEquals(2014, v.properties("age").valueMap().next().get("changeDate"));
    }

    @Test
    public void shouldRemoveMultiPropertiesWhenVerticesAreRemoved() {
        Vertex marko = g.addVertex("name", "marko", "name", "okram");
        Vertex stephen = g.addVertex("name", "stephen", "name", "spmallette");
        assertEquals(2, g.V().count().next().intValue());
        assertEquals(0, g.E().count().next().intValue());
        assertEquals(2, marko.properties("name").count().next().intValue());
        assertEquals(2, stephen.properties("name").count().next().intValue());
        assertEquals(2, marko.properties().count().next().intValue());
        assertEquals(2, stephen.properties().count().next().intValue());
        assertEquals(0, marko.properties("blah").count().next().intValue());
        assertEquals(0, stephen.properties("blah").count().next().intValue());
        assertEquals(2, g.V().count().next().intValue());
        assertEquals(0, g.E().count().next().intValue());

        stephen.remove();
        assertEquals(1, g.V().count().next().intValue());
        assertEquals(0, g.E().count().next().intValue());
        assertEquals(2, marko.properties("name").count().next().intValue());
        assertEquals(2, marko.properties().count().next().intValue());
        assertEquals(0, marko.properties("blah").count().next().intValue());

        for (int i = 0; i < 100; i++) {
            marko.property("name", i);
        }
        assertEquals(1, g.V().count().next().intValue());
        assertEquals(0, g.E().count().next().intValue());
        assertEquals(102, marko.properties("name").count().next().intValue());
        assertEquals(102, marko.properties().count().next().intValue());
        assertEquals(0, marko.properties("blah").count().next().intValue());
        g.V().properties("name").has(MetaProperty.VALUE, (a, b) -> ((Class) b).isAssignableFrom(a.getClass()), Integer.class).remove();
        assertEquals(1, g.V().count().next().intValue());
        assertEquals(0, g.E().count().next().intValue());
        assertEquals(2, marko.properties("name").count().next().intValue());
        assertEquals(2, marko.properties().count().next().intValue());
        assertEquals(0, marko.properties("blah").count().next().intValue());

        marko.remove();
        assertEquals(0, g.V().count().next().intValue());
        assertEquals(0, g.E().count().next().intValue());
        /*
        TODO: Stephen, Neo4j and TinkerGraph have different (though valid) behaviors here. Thoughts?
        assertEquals(0, marko.properties("name").count().next().intValue());
        assertEquals(0, marko.properties().count().next().intValue());
        assertEquals(0, marko.properties("blah").count().next().intValue());*/


    }

    @Test
    public void shouldHandleMetaPropertyTraversals() {
        Vertex v = g.addVertex("i", 1, "i", 2, "i", 3);
        assertEquals(3, v.properties().count().next().intValue());
        assertEquals(3, v.properties("i").count().next().intValue());
        v.properties("i").sideEffect(m -> m.get().<Object>property("aKey", "aValue")).iterate();
        v.properties("i").properties("aKey").forEach(p -> assertEquals("aValue", p.value()));
        assertEquals(3, v.properties("i").properties("aKey").count().next().intValue());
        assertEquals(3, g.V().properties("i").properties("aKey").count().next().intValue());
        assertEquals(1, g.V().properties("i").has(MetaProperty.VALUE, 1).properties("aKey").count().next().intValue());
        assertEquals(3, g.V().properties("i").has(MetaProperty.KEY, "i").properties().count().next().intValue());
    }

    @Test
    public void shouldHandleSingleMetaProperties() {
        Vertex v = g.addVertex("name", "marko", "name", "marko a. rodriguez", "name", "marko rodriguez");
        assertEquals(3, v.properties().count().next().intValue());
        v.singleProperty("name", "okram", "acl", "private", "date", 2014);
        assertEquals(1, v.properties().count().next().intValue());
        assertEquals(2, v.property("name").valueMap().next().size());
        assertEquals("private", v.property("name").valueMap().next().get("acl"));
        assertEquals(2014, v.property("name").valueMap().next().get("date"));
    }
}
