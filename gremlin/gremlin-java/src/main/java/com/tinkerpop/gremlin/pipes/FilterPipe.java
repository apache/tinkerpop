package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.pipes.util.PipelineHelper;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FilterPipe<S> extends AbstractPipe<S, S> {

    private final Predicate<Holder<S>> predicate;

    public FilterPipe(final Pipeline pipeline, final Predicate<Holder<S>> predicate) {
        super(pipeline);
        this.predicate = predicate;
    }

    public Holder<S> processNextStart() {
        while (true) {
            final Holder<S> holder = this.starts.next();
            if (this.predicate.test(holder)) {
                holder.setFuture(PipelineHelper.getNextPipeLabel(this.pipeline, this));
                return holder;
            }
        }
    }
}
