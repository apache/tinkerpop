package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyPropertiesTest {

    public static class StandardTest extends PropertiesTest {

        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value() {
            g.V.has('age').properties('name', 'age').value;
        }

        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value() {
            g.V.has('age').properties('age', 'name').value;
        }

    }

    public static class ComputerTest extends PropertiesTest {

        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value() {
            ComputerTestHelper.compute("g.V.has('age').properties('name', 'age').value", g);
        }

        public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value() {
            ComputerTestHelper.compute("g.V.has('age').properties('age', 'name').value", g);
        }

    }
}
