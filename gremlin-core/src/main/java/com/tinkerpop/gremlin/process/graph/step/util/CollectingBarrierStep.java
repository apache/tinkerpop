package com.tinkerpop.gremlin.process.graph.step.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.step.AbstractStep;
import com.tinkerpop.gremlin.process.util.tool.TraverserSet;

import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CollectingBarrierStep<S> extends AbstractStep<S, S> {
    private TraverserSet<S> traverserSet = new TraverserSet<>();
    private Consumer<TraverserSet<S>> barrierConsumer;

    public CollectingBarrierStep(final Traversal traversal) {
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
            this.starts.forEachRemaining(this.traverserSet::add);
            if (null != this.barrierConsumer) this.barrierConsumer.accept(this.traverserSet);
        }
        return this.traverserSet.remove();
    }

    @Override
    public CollectingBarrierStep<S> clone() throws CloneNotSupportedException {
        final CollectingBarrierStep<S> clone = (CollectingBarrierStep<S>) super.clone();
        clone.traverserSet = new TraverserSet<>();
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.traverserSet.clear();
    }
}
