package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collection;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RetainStep<S> extends FilterStep<S> implements Reversible {

    public final String collectionAs;

    public RetainStep(final Traversal traversal, final String collectionAs) {
        super(traversal);
        this.collectionAs = collectionAs;
        this.setPredicate(traverser -> {
            if (!this.traversal.sideEffects().exists(this.collectionAs))
                return false;
            else {
                final Object retain = this.traversal.sideEffects().get(this.collectionAs);
                return retain instanceof Collection ?
                        ((Collection) retain).contains(traverser.get()) :
                        retain.equals(traverser.get());
            }
        });
    }

    public RetainStep(final Traversal traversal, final Collection<S> retainCollection) {
        super(traversal);
        this.collectionAs = null;
        this.setPredicate(traverser -> retainCollection.contains(traverser.get()));
    }

    public RetainStep(final Traversal traversal, final S retainObject) {
        super(traversal);
        this.collectionAs = null;
        this.setPredicate(traverser -> retainObject.equals(traverser.get()));
    }

    public String toString() {
        return null == this.collectionAs ?
                super.toString() :
                TraversalHelper.makeStepString(this, this.collectionAs);
    }
}
