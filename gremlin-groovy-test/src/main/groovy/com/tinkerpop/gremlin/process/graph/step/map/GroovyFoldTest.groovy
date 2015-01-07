package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
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
        public Traversal<Vertex, Integer> get_g_V_age_foldX0_plusX() {
            g.V.age.fold(0) { seed, age -> seed + age };
        }
    }

    public static class ComputerTest extends FoldTest {
        @Override
        public Traversal<Vertex, List<Vertex>> get_g_V_fold() {
            ComputerTestHelper.compute("g.V.fold", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_fold_unfold() {
            ComputerTestHelper.compute("g.V.fold.unfold", g)
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_foldX0_plusX() {
            ComputerTestHelper.compute("g.V.age.fold(0) { seed, age -> seed + age }", g);
        }
    }
}
