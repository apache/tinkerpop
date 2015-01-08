package com.tinkerpop.gremlin.process.graph.step.branch

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyUnionTest {

    public static class StandardTest extends UnionTest {

        public Traversal<Vertex, String> get_g_V_unionXout_inX_name() {
            g.V.union(__.out, __.in).name
        }
    }

    public static class ComputerTest extends UnionTest {

        public Traversal<Vertex, String> get_g_V_unionXout_inX_name() {
            ComputerTestHelper.compute("g.V.union(__.out, __.in).name", g);
        }
    }
}
