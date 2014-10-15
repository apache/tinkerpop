package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.GlobalMetrics;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FilterStep<S> extends AbstractStep<S, S> {

    private Predicate<Traverser<S>> predicate = null;

    public FilterStep(final Traversal traversal) {
        super(traversal);
    }

    public void setPredicate(final Predicate<Traverser<S>> predicate) {
        this.predicate = predicate;
    }

    @Override
    protected Traverser<S> processNextStart() {
        while (true) {
            final Traverser.Admin<S> traverser = this.starts.next();
            
            if (this.isProfilingEnabled) GlobalMetrics.start(this, traverser);

            if (this.predicate.test(traverser)) {
                if (this.isProfilingEnabled) GlobalMetrics.finish(this, traverser);
                return traverser;
            }

            if (this.isProfilingEnabled) GlobalMetrics.stop(this, traverser);
        }
    }
}
