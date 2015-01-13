package com.tinkerpop.gremlin.process.graph.step.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Barrier;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.process.util.TraverserSet;

import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class BarrierStep<S> extends AbstractStep<S, S> implements Barrier {
    private TraverserSet<S> traverserSet = new TraverserSet<>();
    private Consumer<TraverserSet<S>> barrierConsumer;

    public BarrierStep(final Traversal traversal) {
        super(traversal);
    }

    public void setConsumer(final Consumer<TraverserSet<S>> barrierConsumer) {
        this.barrierConsumer = barrierConsumer;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.BULK);
    }

    @Override
    public Traverser<S> processNextStart() {
        if (this.starts.hasNext()) {
            if (PROFILING_ENABLED) TraversalMetrics.start(this);
            this.starts.forEachRemaining(this.traverserSet::add);
            this.barrierConsumer.accept(this.traverserSet);
        } else {
            if (PROFILING_ENABLED) TraversalMetrics.start(this);
        }

        final Traverser.Admin<S> traverser = this.traverserSet.remove().split();
        if (PROFILING_ENABLED) TraversalMetrics.finish(this, traverser);
        return traverser;

    }

    @Override
    public BarrierStep<S> clone() throws CloneNotSupportedException {
        final BarrierStep<S> clone = (BarrierStep<S>) super.clone();
        clone.traverserSet = new TraverserSet<>();
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.traverserSet.clear();
    }
}
