package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class PropertiesTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value();

    public abstract Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value();

    public abstract Traversal<Vertex, String> get_g_V_propertiesXlocationX_orderByXvalueX_rangeX0_2X_local_value();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_propertiesXname_ageX_value() {
        Arrays.asList(get_g_V_hasXageX_propertiesXage_nameX_value(), get_g_V_hasXageX_propertiesXname_ageX_value()).forEach(traversal -> {
            printTraversalForm(traversal);
            checkResults(Arrays.asList("marko", 29, "vadas", 27, "josh", 32, "peter", 35), traversal);
        });
    }

    @Test
    @LoadGraphWith(CREW)
    public void g_V_propertiesXlocationX_orderByXvalueX_rangeX0_2X_local_value() {
        final Traversal<Vertex, String> traversal = get_g_V_propertiesXlocationX_orderByXvalueX_rangeX0_2X_local_value();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("brussels", "san diego", "centreville", "dulles", "baltimore", "bremen", "aachen", "kaiserslautern"), traversal);

    }

    public static class StandardTest extends PropertiesTest {

        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value() {
            return g.V().has("age").properties("name", "age").value();
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value() {
            return g.V().has("age").properties("age", "name").value();
        }

        @Override
        public Traversal<Vertex, String> get_g_V_propertiesXlocationX_orderByXvalueX_rangeX0_2X_local_value() {
            return g.V().properties("location").orderBy(T.value).range(0, 2).local().value();
        }

    }

    public static class ComputerTest extends PropertiesTest {

        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value() {
            return g.V().has("age").properties("name", "age").value().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value() {
            return g.V().has("age").properties("age", "name").value().submit(g.compute());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_propertiesXlocationX_orderByXvalueX_rangeX0_2X_local_value() {
            return g.V().properties("location").orderBy(T.value).range(0, 2).local().<String>value(); // TODO:
        }

    }

}

