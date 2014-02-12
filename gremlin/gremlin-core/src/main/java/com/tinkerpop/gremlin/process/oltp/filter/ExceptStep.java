package com.tinkerpop.gremlin.process.oltp.filter;

import com.tinkerpop.gremlin.process.Traversal;

import java.util.Collection;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExceptStep<S> extends FilterStep<S> {

    public ExceptStep(final Traversal traversal, final String variable) {
        super(traversal);
        final Object temp = this.traversal.memory().get(variable);
        if (temp instanceof Collection)
            this.setPredicate(holder -> !((Collection) temp).contains(holder.get()));
        else
            this.setPredicate(holder -> !temp.equals(holder.get()));
    }

    public ExceptStep(final Traversal traversal, final Collection<S> exceptionCollection) {
        super(traversal);
        this.setPredicate(holder -> !exceptionCollection.contains(holder.get()));
    }

    public ExceptStep(final Traversal traversal, final S exceptionObject) {
        super(traversal);
        this.setPredicate(holder -> !exceptionObject.equals(holder.get()));
    }
}
