package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collection;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ExceptStep<S> extends FilterStep<S> implements Reversible {

    private final String collectionSideEffectKey;

    public ExceptStep(final Traversal traversal, final String collectionSideEffectKey) {
        super(traversal);
        this.collectionSideEffectKey = collectionSideEffectKey;
        this.setPredicate(traverser -> {
            if (!this.traversal.sideEffects().exists(this.collectionSideEffectKey)) return true;
            else {
                final Object except = this.traversal.sideEffects().get(this.collectionSideEffectKey);
                return except instanceof Collection ?
                        !((Collection) except).contains(traverser.get()) :
                        !except.equals(traverser.get());
            }
        });
    }

    public ExceptStep(final Traversal traversal, final Collection<S> exceptionCollection) {
        super(traversal);
        this.collectionSideEffectKey = null;
        this.setPredicate(traverser -> !exceptionCollection.contains(traverser.get()));
    }

    public ExceptStep(final Traversal traversal, final S exceptionObject) {
        super(traversal);
        this.collectionSideEffectKey = null;
        this.setPredicate(traverser -> !exceptionObject.equals(traverser.get()));
    }

    public String toString() {
        return null == this.collectionSideEffectKey ?
                super.toString() :
                TraversalHelper.makeStepString(this, this.collectionSideEffectKey);
    }
}
