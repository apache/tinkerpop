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
public class UnionPipe<S, E> extends AbstractPipe<S, E> {

    public final PipelineRing<S, E> pipelineRing;

    @SafeVarargs
    public UnionPipe(final Traversal pipeline, final Traversal<S, E>... pipelines) {
        super(pipeline);
        this.pipelineRing = new PipelineRing<>(pipelines);
    }

    protected Holder<E> processNextStart() {
        while (true) {
            int counter = 0;
            while (counter++ < this.pipelineRing.size()) {
                final Traversal<S, E> p = this.pipelineRing.next();
                if (p.hasNext()) return GremlinHelper.getEnd(p).next();
            }
            final Holder<S> start = this.starts.next();
            this.pipelineRing.forEach(p -> p.addStarts(new SingleIterator<>(start.makeSibling())));
        }
    }
}
