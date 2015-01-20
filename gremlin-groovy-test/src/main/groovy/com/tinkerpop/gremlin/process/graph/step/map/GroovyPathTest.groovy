package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Path
import com.tinkerpop.gremlin.process.T
import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyPathTest {

    public static class StandardTest extends PathTest {

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_name_path(final Object v1Id) {
            g.V(v1Id).identity.name.path
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_out_path_byXageX_byXnameX(final Object v1Id) {
            g.V(v1Id).out.path.by('age').by('name');
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_timesX2X_path_by_byXnameX_byXlangX() {
            g.V.repeat(__.out).times(2).path.by.by('name').by('lang');
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_out_path_byXnameX_byXageX() {
            g.V.out.out.path.by('name').by('age');
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXaX_hasXname_markoX_asXbX_hasXage_29X_asXcX_path() {
            g.V.as('a').has('name', 'marko').as('b').has('age', 29).as('c').path;
        }
    }

    public static class ComputerTest extends PathTest {

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_name_path(final Object v1Id) {
            ComputerTestHelper.compute("g.V(${v1Id}).identity.name.path", g);
        }

        @Override
        public Traversal<Vertex, Path> get_g_VX1X_out_path_byXageX_byXnameX(final Object v1Id) {
            g.V(v1Id).out.path.by('age').by('name');
            // TODO
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_repeatXoutX_timesX2X_path_by_byXnameX_byXlangX() {
            g.V.repeat(__.out).times(2).path.by.by('name').by('lang');
            //TODO
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_out_path_byXnameX_byXageX() {
            g.V.out.out.path.by('name').by('age');
            // TODO
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_asXaX_hasXname_markoX_asXbX_hasXage_29X_asXcX_path() {
            ComputerTestHelper.compute("g.V.as('a').has('name', 'marko').as('b').has('age', 29).as('c').path", g);
        }
    }
}
