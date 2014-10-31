package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyPathTest {

    public static class StandardTest extends PathTest {

        @Override
        public Traversal<Vertex, Path> get_g_v1_name_path(final Object v1Id) {
            g.v(v1Id).identity.name.path
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_out_pathXage_nameX(final Object v1Id) {
            g.v(v1Id).out.path { it.age } { it.name }
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2X_pathXit__name__langX() {
            g.V().as('x').out.jump('x') { it.loops() < 2 }.path { it } { it.name } { it.lang }
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_2X_pathXit_name_langX() {
            g.V.as('x').out.jump('x', 2).path { it } { it.name } { it.lang }
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_out_pathXname_ageX() {
            g.V.out.out.path { it.name } { it.age }
        }
    }

    public static class ComputerTest extends PathTest {

        @Override
        public Traversal<Vertex, Path> get_g_v1_name_path(final Object v1Id) {
            ComputerTestHelper.compute("g.v(${v1Id}).identity.name.path", g);
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_out_pathXage_nameX(final Object v1Id) {
            g.v(v1Id).out.path { it.age } { it.name }
            // TODO:ComputerTestHelper.compute("g.v(${v1Id}).out.path { it.age } { it.name }", g);
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2X_pathXit__name__langX() {
            g.V().as('x').out.jump('x') { it.loops() < 2 }.path { it } { it.name } { it.lang }
            // TODO: ComputerTestHelper.compute("g.V().as('x').out.jump('x') { it.loops < 2 }.path { it } { it.name } { it.lang }", g);
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_2X_pathXit_name_langX() {
            g.V.as('x').out.jump('x', 2).path { it } { it.name } { it.lang }
            //TODO: ComputerTestHelper.compute("g.V.as('x').out.jump('x', 2).path { it } { it.name } { it.lang }", g);
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_out_pathXname_ageX() {
            g.V.out.out.path { it.name } { it.age }
            // TODO: ComputerTestHelper.compute("g.V.out.out.path { it.name } { it.age }",g);
        }
    }
}
