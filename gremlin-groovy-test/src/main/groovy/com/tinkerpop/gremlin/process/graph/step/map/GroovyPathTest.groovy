package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

import java.util.function.Function

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyPathTest {

    public static class StandardTest extends PathTest {

        @Override
        public Traversal<Vertex, Path> get_g_v1_name_path(final Object v1Id) {
            g.V(v1Id).identity.name.path
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_out_path_byXageX_byXnameX(final Object v1Id) {
            g.V(v1Id).out.path.by('age').by('name');
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2X_path_byXitX_byXnameX_byXlangX() {
            g.V().as('x').out.jump('x') { it.loops() < 2 }.path.by{it}.by('name').by('lang');
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_2X_path_byXitX_byXnameX_byXlangX() {
            g.V.as('x').out.jump('x', 2).path.by{it}.by('name').by('lang');
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_out_path_byXnameX_byXageX() {
            g.V.out.out.path.by('name').by('age');
        }
    }

    public static class ComputerTest extends PathTest {

        @Override
        public Traversal<Vertex, Path> get_g_v1_name_path(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).identity.name.path", g);
        }

        @Override
        public Traversal<Vertex, Path> get_g_v1_out_path_byXageX_byXnameX(final Object v1Id) {
            g.V(v1Id).out.path.by('age').by('name');
            // TODO
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_loops_lt_2X_path_byXitX_byXnameX_byXlangX() {
            g.V().as('x').out.jump('x') { it.loops() < 2 }.path.by{it}.by('name').by('lang');
            // TODO
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXxX_out_jumpXx_2X_path_byXitX_byXnameX_byXlangX() {
            g.V.as('x').out.jump('x', 2).path.by{it}.by('name').by('lang');
            //TODO
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_out_path_byXnameX_byXageX() {
            g.V.out.out.path.by('name').by('age');
            // TODO
        }
    }
}
