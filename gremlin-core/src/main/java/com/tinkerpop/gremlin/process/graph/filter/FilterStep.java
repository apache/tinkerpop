package com.tinkerpop.gremlin.process.graph.filter;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.util.function.SPredicate;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FilterStep<S> extends AbstractStep<S, S> {

    public SPredicate<Holder<S>> predicate;

    public FilterStep(final Traversal traversal, final SPredicate<Holder<S>> predicate) {
        super(traversal);
        this.predicate = predicate;
    }

    public FilterStep(final Traversal traversal) {
        super(traversal);
    }

    public void setPredicate(final SPredicate<Holder<S>> predicate) {
        this.predicate = predicate;
    }

    public Holder<S> processNextStart() {
        while (true) {
            final Holder<S> holder = this.starts.next();
            if (this.predicate.test(holder)) return holder;
        }
    }
}
