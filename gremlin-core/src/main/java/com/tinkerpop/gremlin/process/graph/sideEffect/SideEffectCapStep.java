package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.SimpleHolder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectCapStep<S, E> extends AbstractStep<S, E> {

    private boolean done = false;

    public SideEffectCapStep(final Traversal traversal) {
        super(traversal);
    }

    public Holder<E> processNextStart() {
        if (!this.done) {
            Holder<E> holder = new SimpleHolder<>((E) NO_OBJECT);
            try {
                while (true) {
                    holder = (Holder<E>) this.starts.next();
                }
            } catch (final NoSuchElementException e) {
            }
            this.done = true;
            return holder.makeChild(this.getAs(), this.traversal.memory().get(SideEffectCapable.CAP_VARIABLE));
        } else {
            throw FastNoSuchElementException.instance();
        }
    }
}
