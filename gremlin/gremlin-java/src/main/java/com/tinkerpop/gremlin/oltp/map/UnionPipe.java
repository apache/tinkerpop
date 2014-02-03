package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.AbstractPipe;
import com.tinkerpop.gremlin.util.GremlinHelper;
import com.tinkerpop.gremlin.util.PipelineRing;
import com.tinkerpop.gremlin.util.SingleIterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UnionPipe<S, E> extends AbstractPipe<S, E> {

    public final PipelineRing<S, E> pipelineRing;

    @SafeVarargs
    public UnionPipe(final Pipeline pipeline, final Pipeline<S, E>... pipelines) {
        super(pipeline);
        this.pipelineRing = new PipelineRing<>(pipelines);
    }

    protected Holder<E> processNextStart() {
        while (true) {
            int counter = 0;
            while (counter++ < this.pipelineRing.size()) {
                final Pipeline<S, E> p = this.pipelineRing.next();
                if (p.hasNext()) {
                    final Holder<E> holder = GremlinHelper.getEnd(p).next();
                    holder.setFuture(this.getNextPipe().getAs());
                    return holder;
                }
            }
            final Holder<S> start = this.starts.next();
            this.pipelineRing.forEach(p -> p.addStarts(new SingleIterator<>(start.makeSibling())));
        }

    }
}
