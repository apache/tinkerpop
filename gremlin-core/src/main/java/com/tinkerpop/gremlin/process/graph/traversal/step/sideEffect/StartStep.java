package com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.traversal.step.AbstractStep;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StartStep<S> extends AbstractStep<S, S> implements Reversible {

    protected Object start;
    protected boolean first = true;

    public StartStep(final Traversal traversal, final Object start) {
        super(traversal);
        this.start = start;
    }

    public StartStep(final Traversal traversal) {
        this(traversal, null);
    }

    public <T> T getStart() {
        return (T) this.start;
    }

    public boolean startAssignableTo(final Class... assignableClasses) {
        return Stream.of(assignableClasses).filter(check -> check.isAssignableFrom(this.start.getClass())).findAny().isPresent();
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.start);
    }

    @Override
    protected Traverser<S> processNextStart() {
        if (this.first) {
            if (null != this.start) {
                if (this.start instanceof Iterator)
                    this.starts.add(this.getTraversal().asAdmin().getTraverserGenerator().generateIterator((Iterator<S>) this.start, this, 1l));
                else
                    this.starts.add(this.getTraversal().asAdmin().getTraverserGenerator().generate((S) this.start, this, 1l));
            }
            this.first = false;
        }
        return this.starts.next();
    }

    @Override
    public StartStep<S> clone() throws CloneNotSupportedException {
        final StartStep<S> clone = (StartStep<S>) super.clone();
        clone.first = true;
        clone.start = null;
        return clone;
    }
}
