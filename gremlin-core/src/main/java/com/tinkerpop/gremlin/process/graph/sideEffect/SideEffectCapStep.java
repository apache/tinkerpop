package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Traverser;
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

    public Traverser<E> processNextStart() {
        if (!this.done) {
            Traverser<E> traverser = new SimpleTraverser<>((E) NO_OBJECT);
            try {
                while (true) {
                    traverser = (Traverser<E>) this.starts.next();
                }
            } catch (final NoSuchElementException e) {
            }
            this.done = true;
            return traverser.makeChild(this.getAs(), this.traversal.memory().get(SideEffectCapable.CAP_VARIABLE));
        } else {
            throw FastNoSuchElementException.instance();
        }
    }
}
