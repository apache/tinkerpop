package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;

import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SingleGraphTraversal<S> implements GraphTraversal<S, S> {

    final S s;
    boolean done = false;

    public SingleGraphTraversal(final S s) {
        this.s = s;
    }

    @Override
    public Variables memory() {
        return null;
    }

    @Override
    public TraversalStrategies optimizers() {
        return null;
    }

    @Override
    public void addStarts(Iterator<Holder<S>> starts) {

    }

    @Override
    public <S, E, T extends Traversal<S, E>> T addStep(Step<?, E> step) {
        return null;
    }

    @Override
    public List<Step> getSteps() {
        return null;
    }

    @Override
    public Traversal<S, S> submit(TraversalEngine engine) {
        return null;
    }

    @Override
    public boolean hasNext() {
        return !done;
    }

    @Override
    public S next() {
        if (!done) {
            this.done = true;
            return this.s;
        } else {
            throw FastNoSuchElementException.instance();
        }
    }
}
