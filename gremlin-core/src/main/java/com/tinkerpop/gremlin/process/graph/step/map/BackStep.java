package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class BackStep<S, E> extends MapStep<S, E> implements EngineDependent {

    private final String stepLabel;
    private boolean requiresPaths = false;

    public BackStep(final Traversal traversal, final String stepLabel) {
        super(traversal);
        this.stepLabel = stepLabel;
        this.setFunction(traverser -> traverser.path(this.stepLabel));
    }

    @Override
    public void onEngine(final TraversalEngine traversalEngine) {
        this.requiresPaths = traversalEngine.equals(TraversalEngine.COMPUTER);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        // TODO: return this.requiresPaths ? Collections.singleton(TraverserRequirement.PATH) : Collections.singleton(TraverserRequirement.PATH_ACCESS);
        return Collections.singleton(TraverserRequirement.PATH);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.stepLabel);
    }
}
