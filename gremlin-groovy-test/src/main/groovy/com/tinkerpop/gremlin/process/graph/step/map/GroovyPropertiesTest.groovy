package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyPropertiesTest {

    public static class StandardTest extends PropertiesTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value() {
            g.V.has('age').properties('name', 'age').value;
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value() {
            g.V.has('age').properties('age', 'name').value;
        }

        @Override
        public Traversal<Vertex, String> get_g_V_propertiesXlocationX_orderByXvalueX_rangeX0_2X_local_value() {
            g.V.local{g.of().properties('location').orderBy(T.value).range(0, 2)}.value
        }


    }

    public static class ComputerTest extends PropertiesTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value() {
            ComputerTestHelper.compute("g.V.has('age').properties('name', 'age').value", g);
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value() {
            ComputerTestHelper.compute("g.V.has('age').properties('age', 'name').value", g);
        }

        @Override
        public Traversal<Vertex, String> get_g_V_propertiesXlocationX_orderByXvalueX_rangeX0_2X_local_value() {
            // ComputerTestHelper.compute("g.V.properties('location').orderBy(T.value).range(0, 2).local().value", g);
            g.V.local{g.of().properties('location').orderBy(T.value).range(0, 2)}.value //TODO
        }

    }

}
