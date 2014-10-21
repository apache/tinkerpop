package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySideEffectTest {

    public static class StandardTest extends SideEffectTest {

        @Override
        public Traversal<Vertex, String> get_g_v1_sideEffectXstore_aX_valueXnameX(final Object v1Id) {
            g.v(v1Id).with('a') { [] }.sideEffect {
                it.a.clear();
                it.a.add(it.get());
            }.name;
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_out_sideEffectXincr_cX_valueXnameX(final Object v1Id) {
            g.v(v1Id).with('c') { [0] }.out.sideEffect {
                def temp = it.c[0];
                it.c.clear();
                it.c.add(temp + 1);
            }.name
        }

        @Override
        public Traversal<Vertex, String> get_g_v1_out_sideEffectXX_valueXnameX(final Object v1Id) {
            g.v(v1Id).out().sideEffect {}.name
        }
    }
}
