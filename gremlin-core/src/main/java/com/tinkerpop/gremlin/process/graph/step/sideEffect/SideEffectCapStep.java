package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCap;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectCapStep<S, E> extends AbstractStep<S, E> implements SideEffectCap {

    private boolean done = false;
    public String sideEffectAs;

    public SideEffectCapStep(final Traversal traversal, final String sideEffectAs) {
        super(traversal);
        this.sideEffectAs = sideEffectAs;
    }

    public Traverser<E> processNextStart() {
        if (!this.done) {
            Traverser<E> traverser = new SimpleTraverser<>((E) NO_OBJECT);
            try {
                while (true) {
                    traverser = (Traverser<E>) this.starts.next();
                }
            } catch (final NoSuchElementException ignored) {
            }
            this.done = true;
            return traverser.makeChild(this.getAs(), this.traversal.memory().<E>get(this.sideEffectAs).get());
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    public String toString() {
        return Graph.Key.isHidden(this.sideEffectAs) ? super.toString() : TraversalHelper.makeStepString(this, this.sideEffectAs);
    }

    public String getVariable() {
        return this.sideEffectAs;
    }
}
