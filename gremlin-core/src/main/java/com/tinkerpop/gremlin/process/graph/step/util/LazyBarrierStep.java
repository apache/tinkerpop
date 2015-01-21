package com.tinkerpop.gremlin.process.graph.step.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Barrier;
import com.tinkerpop.gremlin.process.graph.marker.Reducing;
import com.tinkerpop.gremlin.process.traverser.O_TraverserGenerator;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import org.javatuples.Pair;

import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class LazyBarrierStep<S, E> extends AbstractStep<S, E> implements Barrier {

    private Supplier<E> seedSupplier;
    private BiFunction<E, Traverser<S>, E> barrierFunction;

    public LazyBarrierStep(final Traversal traversal) {
        super(traversal);
    }

    public void setSeedSupplier(final Supplier<E> seedSupplier) {
        this.seedSupplier = seedSupplier;
    }

    public void setFunction(final BiFunction<E, Traverser<S>, E> barrierFunction) {
        this.barrierFunction = barrierFunction;
    }

    public Supplier<E> getSeedSupplier() {
        return this.seedSupplier;
    }

    public BiFunction<E, Traverser<S>, E> getBarrierFunction() {
        return this.barrierFunction;
    }

    @Override
    public Traverser<E> processNextStart() {
        if (this.starts.hasNext()) {
            E seed = this.seedSupplier.get();
            while (this.starts.hasNext()) {
                seed = this.barrierFunction.apply(seed, this.starts.next());
            }
            return this.getTraversal().asAdmin().getTraverserGenerator().generate(seed, (Step) this, 1l);
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    ///////

    public static class ObjectBiFunction<S, E> implements BiFunction<E, Traverser<S>, E> {

        private final BiFunction<E, S, E> biFunction;

        public ObjectBiFunction(final BiFunction<E, S, E> biFunction) {
            this.biFunction = biFunction;
        }

        public final BiFunction<E, S, E> getBiFunction() {
            return this.biFunction;
        }

        @Override
        public E apply(final E seed, final Traverser<S> traverser) {
            return this.biFunction.apply(seed, traverser.get());
        }
    }
}
