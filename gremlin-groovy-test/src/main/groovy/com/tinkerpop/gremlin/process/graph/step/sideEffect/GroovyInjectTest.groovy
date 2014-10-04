package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyInjectTest {

    public static class StandardTest extends InjectTest {
        @Override
        public Traversal<Vertex, String> get_g_v1_out_injectXv2X_name(final Object v1Id, final Object v2Id) {
            g.v(v1Id).out.inject(g.v(v2Id)).name
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_out_name_injectXdanielX_asXaX_mapXlengthX_path(final Object v1Id) {
            g.v(v1Id).out().value('name').inject('daniel').as('a').map { it.length() }.path
        }
    }

    public static class ComputerTest extends InjectTest {
        @Override
        public Traversal<Vertex, String> get_g_v1_out_injectXv2X_name(final Object v1Id, final Object v2Id) {
            g.v(v1Id).out.inject(g.v(v2Id)).name
            // TODO: ComputerTestHelper.compute("g.v(${v1Id}).out.inject(g.v(${v2Id})).name", g);
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_out_name_injectXdanielX_asXaX_mapXlengthX_path(final Object v1Id) {
            g.v(v1Id).out().value('name').inject('daniel').as('a').map { it.length() }.path
            // TODO: ComputerTestHelper.compute("g.v(${v1Id}).out().value('name').inject('daniel').as('a').map{it.length()}.path", g);
        }
    }
}
