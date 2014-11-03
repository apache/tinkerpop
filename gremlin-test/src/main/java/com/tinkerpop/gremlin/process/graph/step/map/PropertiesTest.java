package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class PropertiesTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value();

    public abstract Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_propertiesXname_ageX_value() {
        Arrays.asList(get_g_V_hasXageX_propertiesXage_nameX_value(), get_g_V_hasXageX_propertiesXname_ageX_value()).forEach(traversal -> {
            printTraversalForm(traversal);
            checkResults(Arrays.asList("marko", 29, "vadas", 27, "josh", 32, "peter", 35), traversal);
        });
    }

    public static class StandardTest extends PropertiesTest {

        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value() {
            return g.V().has("age").properties("name", "age").value();
        }

        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value() {
            return g.V().has("age").properties("age", "name").value();
        }

    }

    public static class ComputerTest extends PropertiesTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value() {
            return g.V().has("age").properties("name", "age").value().submit(g.compute());
        }

        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value() {
            return g.V().has("age").properties("age", "name").value().submit(g.compute());
        }

    }

}

