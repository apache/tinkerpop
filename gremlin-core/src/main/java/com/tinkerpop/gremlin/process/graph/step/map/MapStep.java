package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;

import java.util.Collections;
import java.util.Set;
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
            final Traverser.Admin<E> endTraverser =  traverser.split(end, this);
            if (PROFILING_ENABLED) TraversalMetrics.finish(this, endTraverser);
            return endTraverser;
        }
    }

    public void setFunction(final Function<Traverser<S>, E> function) {
        this.function = function;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.PATH_ACCESS); // TODO: this is bad -- just a hack right now.
    }
}

