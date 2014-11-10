package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapStep<S, E> extends AbstractStep<S, E> {

    private Function<Traverser<S>, E> function = null;

    public MapStep(final Traversal traversal) {
        super(traversal);
    }

    @Override
    protected Traverser<E> processNextStart() {
        while (true) {
            final Traverser.Admin<S> traverser = this.starts.next();
            if (PROFILING_ENABLED) TraversalMetrics.start(this);

            final E end = this.function.apply(traverser);
            if (NO_OBJECT != end) {
                final Traverser.Admin<E> endTraverser = traverser.makeChild(this.label, end);
                if (PROFILING_ENABLED) TraversalMetrics.finish(this, endTraverser);
                return endTraverser;
            }

            if (PROFILING_ENABLED) TraversalMetrics.stop(this);
        }
    }

    public void setFunction(final Function<Traverser<S>, E> function) {
        this.function = function;
    }
}

