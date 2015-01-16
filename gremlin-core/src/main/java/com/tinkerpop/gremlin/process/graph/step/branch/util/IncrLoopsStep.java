package com.tinkerpop.gremlin.process.graph.step.branch.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class IncrLoopsStep<S> extends SideEffectStep<S> {

    public IncrLoopsStep(final Traversal traversal) {
        super(traversal);
        this.setConsumer(traverser -> traverser.asAdmin().incrLoops(this.getId()));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.SINGLE_LOOP);
    }
}
