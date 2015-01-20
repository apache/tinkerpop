package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StartStep<S> extends SideEffectStep<S> implements Reversible {

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

    public void setStart(final Object start) {
        this.start = start;
    }

    public boolean startAssignableTo(final Class... assignableClasses) {
        return Stream.of(assignableClasses).filter(check -> check.isAssignableFrom(this.start.getClass())).findAny().isPresent();
    }

    public String toString() {
        return TraversalHelper.makeStepString(this, this.start);
    }

    public void generateTraversers(final TraverserGenerator traverserGenerator) {
        if (null != this.start) {
            if (this.start instanceof Iterator) {
                this.starts.add(traverserGenerator.generateIterator((Iterator<S>) this.start, this, 1l));
            } else {
                this.starts.add(traverserGenerator.generate((S) this.start, this, 1l));
            }
        }
    }

    @Override
    protected Traverser<S> processNextStart() {
        if (this.first) {
            this.generateTraversers(this.getTraversal().asAdmin().getTraverserGenerator());
            this.first = false;
        }
        return super.processNextStart();
    }
}
