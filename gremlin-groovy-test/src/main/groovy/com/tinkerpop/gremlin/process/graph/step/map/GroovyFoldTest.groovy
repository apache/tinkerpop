package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyFoldTest {

    public static class StandardTest extends FoldTest {
        @Override
        public Traversal<Vertex, List<Vertex>> get_g_V_fold() {
            g.V.fold
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_fold_unfold() {
            g.V.fold.unfold
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_hasXageX_foldX0_plusX() {
            g.V.has('age').fold(0) { seed, v -> seed + v.age };
        }
    }
}
