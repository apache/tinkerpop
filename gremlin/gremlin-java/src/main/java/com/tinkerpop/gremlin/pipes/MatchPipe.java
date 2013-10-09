package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.MultiIterator;
import com.tinkerpop.gremlin.pipes.util.PipelineHelper;
import com.tinkerpop.gremlin.pipes.util.SingleIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MatchPipe<S, E> extends AbstractPipe<S, E> {

    private Iterator<Holder<E>> iterator = Collections.emptyIterator();
    private final Pipeline[] pipelines;
    private final Map<String, List<Pipeline>> predicatePipelines = new HashMap<>();
    private final String inAs;
    private final String outAs;

    public MatchPipe(final String inAs, final String outAs, final Pipeline pipeline, final Pipeline... pipelines) {
        super(pipeline);
        this.inAs = inAs;
        this.outAs = outAs;
        this.pipelines = pipelines;
        for (final Pipeline p1 : this.pipelines) {
            final Pipe endPipe = PipelineHelper.getEnd(p1);
            final String endPipeName = endPipe.getName();
            if (!endPipeName.equals(Pipe.NONE)) {
                for (final Pipeline p2 : this.pipelines) {
                    final Pipe startPipe = PipelineHelper.getStart(p2);
                    if (endPipe.getName().equals(startPipe.getName()))
                        startPipe.addStarts(endPipe);
                }
            } else {
                List<Pipeline> pipes = this.predicatePipelines.get(PipelineHelper.getStart(p1).getName());
                if (null == pipes) {
                    pipes = new ArrayList<>();
                    this.predicatePipelines.put(PipelineHelper.getStart(p1).getName(), pipes);
                }
                pipes.add(p1);
            }
        }
    }

    public Holder<E> processNextStart() {
        while (true) {
            if (this.iterator.hasNext()) {
                // CHECK ALL END PREDICATE PIPELINE
                final Holder<E> holder = this.iterator.next();
                if (isLegalPredicate(this.outAs, holder)) {
                    return holder;
                }
            } else {
                final Holder<S> start = this.starts.next();
                // IF PREDICATES HOLD, DO END-NAMED PIPELINES
                if (isLegalPredicate(this.inAs, start)) {
                    this.getAs(this.inAs).forEach(pipe -> pipe.addStarts(new SingleIterator(start.makeSibling())));
                    this.iterator = new MultiIterator(this.getAs(outAs));
                }
            }
        }
    }

    private List<Pipe> getAs(final String key) {
        return (List) Stream.of(this.pipelines)
                .map(p -> PipelineHelper.getAs(key, p))
                .filter(p -> null != p)
                .collect(Collectors.toList());
    }

    private boolean isLegalPredicate(final String name, final Holder holder) {
        boolean legal = true;
        if (this.predicatePipelines.containsKey(name)) {
            for (final Pipeline pipeline : this.predicatePipelines.get(name)) {
                pipeline.addStarts(new SingleIterator(holder.makeSibling()));
                if (!PipelineHelper.hasNextIteration(pipeline)) {
                    legal = false;
                    break; // short-circuit AND
                }
            }
        }
        return legal;
    }

}
