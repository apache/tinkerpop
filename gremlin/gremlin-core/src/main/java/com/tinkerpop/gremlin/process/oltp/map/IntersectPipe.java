package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.AbstractPipe;
import com.tinkerpop.gremlin.process.oltp.util.PipelineRing;
import com.tinkerpop.gremlin.process.oltp.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.GremlinHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IntersectPipe<S, E> extends AbstractPipe<S, E> {

    public final PipelineRing<S, E> pipelineRing;
    private boolean drainState = false; // TODO: Make an AtomicBoolean?

    @SafeVarargs
    public IntersectPipe(final Traversal pipeline, final Traversal<S, E>... pipelines) {
        super(pipeline);
        this.pipelineRing = new PipelineRing<>(pipelines);
    }

    protected Holder<E> processNextStart() {
        while (true) {
            if (this.drainState) {
                int counter = 0;
                while (counter++ < this.pipelineRing.size()) {
                    final Traversal<S, E> pipeline = this.pipelineRing.next();
                    if (pipeline.hasNext()) {
                        return GremlinHelper.getEnd(pipeline).next();
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
