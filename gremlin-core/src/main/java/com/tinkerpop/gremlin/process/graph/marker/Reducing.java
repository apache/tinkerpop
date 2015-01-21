package com.tinkerpop.gremlin.process.graph.marker;

import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Reducing<A, B> {

    public Reducer<A, B> getReducer();

    public class Reducer<A, B> {
        private final Supplier<A> seedSupplier;
        private final BiFunction<A, B, A> biFunction;
        private final boolean onTraverser;

        public Reducer(final Supplier<A> seedSupplier, final BiFunction<A, B, A> biFunction, final boolean onTraverser) {
            this.seedSupplier = seedSupplier;
            this.biFunction = biFunction;
            this.onTraverser = onTraverser;
        }

        public boolean onTraverser() {
            return this.onTraverser;
        }

        public Supplier<A> getSeedSupplier() {
            return this.seedSupplier;
        }

        public BiFunction<A, B, A> getBiFunction() {
            return this.biFunction;
        }
    }
}
