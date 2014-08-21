package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collection;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExceptStep<S> extends FilterStep<S> implements Reversible {

    public final String collectionAs;

    public ExceptStep(final Traversal traversal, final String collectionAs) {
        super(traversal);
        this.collectionAs = collectionAs;
        this.setPredicate(traverser -> {
            if (!this.traversal.sideEffects().exists(this.collectionAs)) return true;
            else {
                final Object except = this.traversal.sideEffects().get(this.collectionAs);
                return except instanceof Collection ?
                        !((Collection) except).contains(traverser.get()) :
                        !except.equals(traverser.get());
            }
        });
    }

    public ExceptStep(final Traversal traversal, final Collection<S> exceptionCollection) {
        super(traversal);
        this.collectionAs = null;
        this.setPredicate(traverser -> !exceptionCollection.contains(traverser.get()));
    }

    public ExceptStep(final Traversal traversal, final S exceptionObject) {
        super(traversal);
        this.collectionAs = null;
        this.setPredicate(traverser -> !exceptionObject.equals(traverser.get()));
    }

    public String toString() {
        return null == this.collectionAs ?
                super.toString() :
                TraversalHelper.makeStepString(this, this.collectionAs);
    }
}
