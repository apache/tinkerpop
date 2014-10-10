package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
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
    public SideEffects sideEffects() {
        return null;
    }

    @Override
    public Strategies strategies() {
        return null;
    }

    @Override
    public void addStarts(Iterator<Traverser<S>> starts) {

    }

    @Override
    public void addStart(Traverser<S> start) {

    }

    @Override
    public <E2> GraphTraversal<S, E2> addStep(Step<?, E2> step) {
        return null;
    }

    @Override
    public List<Step> getSteps() {
        return null;
    }

    @Override
    public GraphTraversal<S, S> submit(GraphComputer computer) {
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

    @Override
    public SingleGraphTraversal clone() {
        try {
            return (SingleGraphTraversal) super.clone();
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
