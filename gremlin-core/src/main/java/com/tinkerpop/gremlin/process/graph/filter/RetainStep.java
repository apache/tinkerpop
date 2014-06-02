package com.tinkerpop.gremlin.process.graph.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.Reversible;

import java.util.Collection;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RetainStep<S> extends FilterStep<S> implements Reversible {

    public RetainStep(final Traversal traversal, final String variable) {
        super(traversal);
        final Object temp = this.traversal.memory().get(variable);
        if (temp instanceof Collection)
            this.setPredicate(traverser -> ((Collection) temp).contains(traverser.get()));
        else
            this.setPredicate(traverser -> temp.equals(traverser.get()));
    }

    public RetainStep(final Traversal traversal, final Collection<S> retainCollection) {
        super(traversal);
        this.setPredicate(traverser -> retainCollection.contains(traverser.get()));
    }

    public RetainStep(final Traversal traversal, final S retainObject) {
        super(traversal);
        this.setPredicate(traverser -> retainObject.equals(traverser.get()));
    }

}
