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
public abstract class SupplierBarrierStep<S, E> extends AbstractStep<S, E> {

    public Supplier<E> supplier;
    private boolean done = false;

    public SupplierBarrierStep(final Traversal traversal) {
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
    public SupplierBarrierStep<S, E> clone() throws CloneNotSupportedException {
        final SupplierBarrierStep<S, E> clone = (SupplierBarrierStep<S, E>) super.clone();
        clone.done = false;
        clone.supplier = CloneableLambda.cloneOrReturn(this.supplier);
        return clone;
    }


}
