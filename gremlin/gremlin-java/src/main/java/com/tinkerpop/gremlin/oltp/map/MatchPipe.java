package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.AbstractPipe;
import com.tinkerpop.gremlin.util.GremlinHelper;
import com.tinkerpop.gremlin.util.SingleIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MatchPipe<S, E> extends AbstractPipe<S, E> {

    private final Map<String, List<Pipeline>> predicatePipelines = new HashMap<>();
    private final Map<String, List<Pipeline>> internalPipelines = new HashMap<>();
    private Pipeline endPipeline;
    private String endPipelineStartAs;
    private final String inAs;
    private final String outAs;

    public MatchPipe(final Pipeline pipeline, final String inAs, final String outAs, final Pipeline... pipelines) {
        super(pipeline);
        this.inAs = inAs;
        this.outAs = outAs;
        for (final Pipeline p1 : pipelines) {
            final String start = GremlinHelper.getStart(p1).getAs();
            final String end = GremlinHelper.getEnd(p1).getAs();
            if (!GremlinHelper.isLabeled(start)) {
                throw new IllegalStateException("All match pipelines must have their start pipe labeled");
            }
            if (!GremlinHelper.isLabeled(end)) {
                final List<Pipeline> list = this.predicatePipelines.getOrDefault(start, new ArrayList<>());
                this.predicatePipelines.put(start, list);
                list.add(p1);
            } else {
                if (end.equals(this.outAs)) {
                    this.endPipeline = p1;
                    this.endPipelineStartAs = GremlinHelper.getStart(p1).getAs();
                } else {
                    final List<Pipeline> list = this.internalPipelines.getOrDefault(start, new ArrayList<>());
                    this.internalPipelines.put(start, list);
                    list.add(p1);
                }
            }
        }
    }

    public Holder<E> processNextStart() {
        while (true) {
            if (this.endPipeline.hasNext()) {
                final Holder<E> holder = (Holder<E>) GremlinHelper.getEnd(this.endPipeline).next();
                if (doPredicates(this.outAs, holder))
                    return holder;
            } else {
                final Holder temp = this.starts.next();
                temp.getPath().renameLastStep(this.inAs); // TODO: is this cool?
                doMatch(this.inAs, temp);
            }
        }
    }

    private void doMatch(final String as, final Holder holder) {
        if (!doPredicates(as, holder))
            return;

        if (as.equals(this.endPipelineStartAs)) {
            this.endPipeline.addStarts(new SingleIterator<>(holder));
            return;
        }

        for (final Pipeline pipeline : this.internalPipelines.get(as)) {
            pipeline.addStarts(new SingleIterator<>(holder));
            final Pipe<?, ?> endPipe = GremlinHelper.getEnd(pipeline);
            while (endPipe.hasNext()) {
                final Holder temp = endPipe.next();
                doMatch(endPipe.getAs(), temp);
            }
        }
    }

    private boolean doPredicates(final String as, final Holder holder) {
        if (this.predicatePipelines.containsKey(as)) {
            for (final Pipeline pipeline : this.predicatePipelines.get(as)) {
                pipeline.addStarts(new SingleIterator<>(holder));
                if (!GremlinHelper.hasNextIteration(pipeline))
                    return false;
            }
        }
        return true;
    }

}
