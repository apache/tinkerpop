package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traverser;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExpandableStepIterator<E> implements Iterator<Traverser.Admin<E>> {

    private final TraverserSet<E> traverserSet = new TraverserSet<>();
    private final Step<?, E> hostStep;

    public ExpandableStepIterator(final Step<?, E> hostStep) {
        this.hostStep = hostStep;
    }

    @Override
    public boolean hasNext() {
        return !this.traverserSet.isEmpty() || this.hostStep.getPreviousStep().hasNext();
    }

    @Override
    public Traverser.Admin<E> next() {
        if (!this.traverserSet.isEmpty())
            return this.traverserSet.remove();

        if (this.hostStep.getPreviousStep().hasNext())
            return (Traverser.Admin<E>) this.hostStep.getPreviousStep().next();
        else
            return this.traverserSet.remove();
    }

    public void add(final Iterator<Traverser.Admin<E>> iterator) {
        iterator.forEachRemaining(this.traverserSet::add);
    }

    @Override
    public String toString() {
        return this.traverserSet.toString();
    }

    public void clear() {
        this.traverserSet.clear();
    }
}
