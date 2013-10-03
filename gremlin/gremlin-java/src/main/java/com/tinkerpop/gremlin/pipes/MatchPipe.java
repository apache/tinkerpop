package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.MultiIterator;
import com.tinkerpop.gremlin.pipes.util.PipelineHelper;
import com.tinkerpop.gremlin.pipes.util.SingleIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MatchPipe<S, E> extends AbstractPipe<S, E> {

    private Iterator<Holder<E>> iterator = Collections.emptyIterator();
    private final Pipeline[] pipelines;
    private final String inAs;
    private final String outAs;

    public MatchPipe(final String inAs, final String outAs, final Pipeline pipeline, final Pipeline... pipelines) {
        super(pipeline);
        this.inAs = inAs;
        this.outAs = outAs;
        this.pipelines = pipelines;
        for (final Pipeline p1 : this.pipelines) {
            final Pipe endPipe = PipelineHelper.getEnd(p1);
            for (final Pipeline p2 : this.pipelines) {
                final Pipe startPipe = PipelineHelper.getStart(p2);
                if (endPipe.getName().equals(startPipe.getName()))
                    startPipe.addStarts(endPipe);
            }
        }
    }

    public Holder<E> processNextStart() {
        while (true) {
            if (this.iterator.hasNext())
                return this.iterator.next();
            else {
                final Holder<S> start = this.starts.next();
                this.getAs(this.inAs).forEach(pipe -> pipe.addStarts(new SingleIterator(start.makeSibling())));
                this.iterator = new MultiIterator(this.getAs(outAs));
            }
        }
    }

    public List<Pipe> getAs(final String key) {
        return (List) Stream.of(this.pipelines)
                .map(p -> PipelineHelper.getAs(key,p))
                .filter(p -> null != p)
                .collect(Collectors.toList());
    }

}
