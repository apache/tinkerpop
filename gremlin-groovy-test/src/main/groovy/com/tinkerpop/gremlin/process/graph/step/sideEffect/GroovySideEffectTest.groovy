package com.tinkerpop.gremlin.process.graph.step.sideEffect

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySideEffectTest {

    public static class StandardTest extends SideEffectTest {

        @Override
        public Traversal<Vertex, String> get_g_VX1X_sideEffectXstore_aX_name(final Object v1Id) {
            g.V(v1Id).withSideEffect('a') { [] }.sideEffect {
                it.sideEffects('a').clear();
                it.sideEffects('a').add(it.get());
            }.name;
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_sideEffectXincr_cX_name(final Object v1Id) {
            g.V(v1Id).withSideEffect('c') { [0] }.out.sideEffect {
                def temp = it.sideEffects('c')[0];
                it.sideEffects('c').clear();
                it.sideEffects('c').add(temp + 1);
            }.name
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_out_sideEffectXX_name(final Object v1Id) {
            g.V(v1Id).out().sideEffect {}.name
        }
    }
}
