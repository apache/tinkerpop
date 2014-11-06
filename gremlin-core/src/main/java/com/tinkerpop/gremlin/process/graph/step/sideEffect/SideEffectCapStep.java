package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.traversers.SimpleTraverser;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SideEffectCapStep<S, E> extends AbstractStep<S, E> implements EngineDependent {

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
            Traverser.Admin<E> traverser = new SimpleTraverser<>((E) NO_OBJECT, this.getTraversal().sideEffects());
            try {
                while (true) {
                    traverser = (Traverser.Admin<E>) this.starts.next();
                }
            } catch (final NoSuchElementException ignored) {
            }

            if (PROFILING_ENABLED) TraversalMetrics.start(this);
            this.done = true;
            traverser.setBulk(1l);
            final Traverser.Admin<E> returnTraverser = traverser.makeChild(this.getLabel(), traverser.sideEffects().<E>get(this.sideEffectKey));
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
    public String toString() {
        return Graph.System.isSystem(this.sideEffectKey) ? super.toString() : TraversalHelper.makeStepString(this, this.sideEffectKey);
    }

    public String getSideEffectKey() {
        return this.sideEffectKey;
    }
}
