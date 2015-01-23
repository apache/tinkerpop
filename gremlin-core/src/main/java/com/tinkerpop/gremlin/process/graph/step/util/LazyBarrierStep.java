package com.tinkerpop.gremlin.process.graph.step.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Barrier;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.util.function.CloneableLambda;

import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class LazyBarrierStep<S, E> extends AbstractStep<S, E> implements Barrier {

    private Supplier<E> seedSupplier;
    private BiFunction<E, Traverser<S>, E> barrierFunction;
    private boolean done = false;

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
    public void reset() {
        super.reset();
        this.done = false;
    }

    @Override
    public Traverser<E> processNextStart() {
        if (this.done)
            throw FastNoSuchElementException.instance();
        E seed = this.seedSupplier.get();
        while (this.starts.hasNext()) {
            seed = this.barrierFunction.apply(seed, this.starts.next());
        }
        this.done = true;
        return this.getTraversal().asAdmin().getTraverserGenerator().generate(seed, (Step) this, 1l);
    }

    @Override
    public LazyBarrierStep<S, E> clone() throws CloneNotSupportedException {
        final LazyBarrierStep<S, E> clone = (LazyBarrierStep<S, E>) super.clone();
        clone.done = false;
        clone.barrierFunction = CloneableLambda.cloneOrReturn(this.barrierFunction);
        return clone;
    }

    ///////

    public static class ObjectBiFunction<S, E> implements BiFunction<E, Traverser<S>, E> { // Cloneable {

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

        /*@Override
        public ObjectBiFunction<S, E> clone() throws CloneNotSupportedException {
            final ObjectBiFunction<S, E> clone = (ObjectBiFunction<S, E>) super.clone();
            clone.biFunction = CloneableLambda.cloneOrReturn(this.biFunction);
            return clone;

        }*/
    }
}
