package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;

import java.util.Arrays;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SideEffectCapStep<S, E> extends AbstractStep<S, E> implements EngineDependent {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.SIDE_EFFECTS,
            TraverserRequirement.OBJECT
    ));

    private boolean done = false;
    private boolean onGraphComputer = false;
    private final String sideEffectKey;

    public SideEffectCapStep(final Traversal traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
    }

    @Override
    public Traverser<E> processNextStart() {
        return this.onGraphComputer ? computerAlgorithm() : standardAlgorithm();
    }

    private Traverser<E> standardAlgorithm() {
        if (!this.done) {
            Traverser.Admin<E> traverser = this.getTraversal().asAdmin().getTraverserGenerator().generate(null, (Step) this, 1l);
            try {
                while (true) {
                    traverser = (Traverser.Admin<E>) this.starts.next();
                }
            } catch (final NoSuchElementException ignored) {
            }

            if (PROFILING_ENABLED) TraversalMetrics.start(this);
            this.done = true;
            traverser.setBulk(1l);
            final Traverser.Admin<E> returnTraverser = traverser.split(traverser.getSideEffects().<E>get(this.sideEffectKey), (Step) this);
            if (PROFILING_ENABLED) TraversalMetrics.finish(this, traverser);
            return returnTraverser;
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    private Traverser<E> computerAlgorithm() {
        while (true) {
            this.starts.next();
        }
    }

    public void onEngine(final TraversalEngine traversalEngine) {
        this.onGraphComputer = traversalEngine.equals(TraversalEngine.COMPUTER);
    }

    @Override
    public void reset() {
        super.reset();
        this.done = false;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey);
    }

    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }
}
