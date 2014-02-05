package com.tinkerpop.gremlin.process.oltp.filter;

import com.tinkerpop.gremlin.process.Traversal;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DedupPipe<S> extends FilterPipe<S> {

    public boolean hasUniqueFunction;

    public DedupPipe(final Traversal pipeline, final Function<S, ?> uniqueFunction) {
        super(pipeline);
        final Set<Object> set = new LinkedHashSet<>();
        if (null == uniqueFunction) {
            this.hasUniqueFunction = false;
            this.setPredicate(holder -> set.add(holder.get()));
        } else {
            this.hasUniqueFunction = true;
            this.setPredicate(holder -> set.add(uniqueFunction.apply(holder.get())));
        }
    }

    public DedupPipe(final Traversal pipeline) {
        this(pipeline, null);
    }
}
