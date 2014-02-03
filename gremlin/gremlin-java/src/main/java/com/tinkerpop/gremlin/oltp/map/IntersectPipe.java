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
public class IntersectPipe<S, E> extends AbstractPipe<S, E> {

    public final PipelineRing<S, E> pipelineRing;
    private boolean drainState = false; // TODO: Make an AtomicBoolean?

    @SafeVarargs
    public IntersectPipe(final Pipeline pipeline, final Pipeline<S, E>... pipelines) {
        super(pipeline);
        this.pipelineRing = new PipelineRing<>(pipelines);
    }

    protected Holder<E> processNextStart() {
        while (true) {
            if (this.drainState) {
                int counter = 0;
                while (counter++ < this.pipelineRing.size()) {
                    final Pipeline<S, E> pipeline = this.pipelineRing.next();
                    if (pipeline.hasNext()) {
                        final Holder<E> holder = GremlinHelper.getEnd(pipeline).next();
                        holder.setFuture(this.getNextPipe().getAs());
                        return holder;
                    }
                }
                this.drainState = false;
                this.pipelineRing.reset();
            } else {
                final Holder<S> start = this.starts.next();
                this.pipelineRing.forEach(p -> p.addStarts(new SingleIterator<>(start.makeSibling())));
                if (this.pipelineRing.stream().map(p -> p.hasNext()).reduce(true, (a, b) -> a && b))
                    this.drainState = true;
                else
                    this.pipelineRing.stream().forEach(GremlinHelper::iterate);
            }
        }
    }
}
