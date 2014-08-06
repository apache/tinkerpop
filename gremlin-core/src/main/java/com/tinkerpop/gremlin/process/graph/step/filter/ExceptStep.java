package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collection;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExceptStep<S> extends FilterStep<S> implements Reversible {

    public final String collectionAs;

    public ExceptStep(final Traversal traversal, final String collectionAs) {
        super(traversal);
        this.collectionAs = collectionAs;
        this.setPredicate(traverser -> {
            final Optional optional = this.traversal.memory().get(this.collectionAs);
            if (!optional.isPresent()) return true;
            else {
                final Object except = optional.get();
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
