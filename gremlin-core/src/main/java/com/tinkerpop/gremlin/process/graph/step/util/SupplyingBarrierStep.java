package com.tinkerpop.gremlin.process.graph.step.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.util.function.CloneableLambda;

import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class SupplyingBarrierStep<S, E> extends AbstractStep<S, E> {

    public Supplier<E> supplier;
    private boolean done = false;

    public SupplyingBarrierStep(final Traversal traversal) {
        super(traversal);
    }

    public void setSupplier(final Supplier<E> supplier) {
        this.supplier = supplier;
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
        while (this.starts.hasNext())
            this.starts.next();
        this.done = true;
        return this.getTraversal().asAdmin().getTraverserGenerator().generate(this.supplier.get(), (Step) this, 1l);
    }

    @Override
    public SupplyingBarrierStep<S, E> clone() throws CloneNotSupportedException {
        final SupplyingBarrierStep<S, E> clone = (SupplyingBarrierStep<S, E>) super.clone();
        clone.done = false;
        clone.supplier = CloneableLambda.tryClone(this.supplier);
        return clone;
    }


}
