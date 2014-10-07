package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collection;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RetainStep<S> extends FilterStep<S> implements Reversible {

    private final String collectionSideEffectKey;

    public RetainStep(final Traversal traversal, final String collectionSideEffectKey) {
        super(traversal);
        this.collectionSideEffectKey = collectionSideEffectKey;
        this.setPredicate(traverser -> {
            if (!this.traversal.sideEffects().exists(this.collectionSideEffectKey))
                return false;
            else {
                final Object retain = this.traversal.sideEffects().get(this.collectionSideEffectKey);
                return retain instanceof Collection ?
                        ((Collection) retain).contains(traverser.get()) :
                        retain.equals(traverser.get());
            }
        });
    }

    public RetainStep(final Traversal traversal, final Collection<S> retainCollection) {
        super(traversal);
        this.collectionSideEffectKey = null;
        this.setPredicate(traverser -> retainCollection.contains(traverser.get()));
    }

    public RetainStep(final Traversal traversal, final S retainObject) {
        super(traversal);
        this.collectionSideEffectKey = null;
        this.setPredicate(traverser -> retainObject.equals(traverser.get()));
    }

    public String toString() {
        return null == this.collectionSideEffectKey ?
                super.toString() :
                TraversalHelper.makeStepString(this, this.collectionSideEffectKey);
    }
}
