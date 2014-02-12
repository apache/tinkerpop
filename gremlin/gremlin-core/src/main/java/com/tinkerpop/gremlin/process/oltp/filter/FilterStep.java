package com.tinkerpop.gremlin.process.oltp.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.oltp.AbstractStep;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FilterStep<S> extends AbstractStep<S, S> {

    protected Predicate<Holder<S>> predicate;

    public FilterStep(final Traversal traversal, final Predicate<Holder<S>> predicate) {
        super(traversal);
        this.predicate = predicate;
    }

    public FilterStep(final Traversal traversal) {
        super(traversal);
    }

    public void setPredicate(final Predicate<Holder<S>> predicate) {
        this.predicate = predicate;
    }

    public Holder<S> processNextStart() {
        while (true) {
            final Holder<S> holder = this.starts.next();
            if (this.predicate.test(holder)) return holder;
        }
    }
}
