package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.TraverserSource;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StartStep<S> extends SideEffectStep<S> implements TraverserSource, Reversible {

    protected Object start;

    public StartStep(final Traversal traversal, final Object start) {
        super(traversal);
        this.start = start;
    }

    public StartStep(final Traversal traversal) {
        this(traversal, null);
    }

    @Override
    public void clear() {
        this.starts.clear();
    }

    public <T> T getStart() {
        return (T) this.start;
    }

    public boolean startAssignableTo(final Class... assignableClasses) {
        return Stream.of(assignableClasses).filter(check -> check.isAssignableFrom(this.start.getClass())).findAny().isPresent();
    }

    public String toString() {
        return null == this.start ? TraversalHelper.makeStepString(this) : TraversalHelper.makeStepString(this, this.start);
    }

    @Override
    public void generateTraversers(final TraverserGenerator traverserGenerator) {
        if (null != this.start) {
            if (this.start instanceof Iterator) {
                this.starts.add(traverserGenerator.generateIterator((Iterator<S>) this.start, this));
            } else {
                this.starts.add(traverserGenerator.generate((S) this.start, this));
            }

        }
    }
}
