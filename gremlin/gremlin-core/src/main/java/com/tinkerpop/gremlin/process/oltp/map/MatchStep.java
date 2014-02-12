package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.AbstractStep;
import com.tinkerpop.gremlin.process.oltp.util.SingleIterator;
import com.tinkerpop.gremlin.process.util.GremlinHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MatchStep<S, E> extends AbstractStep<S, E> {

    private final Map<String, List<Traversal>> predicatePipelines = new HashMap<>();
    private final Map<String, List<Traversal>> internalPipelines = new HashMap<>();
    private Traversal endPipeline = null;
    private String endPipelineStartAs;
    private final String inAs;
    private final String outAs;

    public MatchStep(final Traversal traversal, final String inAs, final String outAs, final Traversal... traversals) {
        super(traversal);
        this.inAs = inAs;
        this.outAs = outAs;
        for (final Traversal p1 : traversals) {
            final String start = GremlinHelper.getStart(p1).getAs();
            final String end = GremlinHelper.getEnd(p1).getAs();
            if (!GremlinHelper.isLabeled(start)) {
                throw new IllegalArgumentException("All match traversals must have their start pipe labeled");
            }
            if (!GremlinHelper.isLabeled(end)) {
                final List<Traversal> list = this.predicatePipelines.getOrDefault(start, new ArrayList<>());
                this.predicatePipelines.put(start, list);
                list.add(p1);
            } else {
                if (end.equals(this.outAs)) {
                    if (null != this.endPipeline)
                        throw new IllegalArgumentException("There can only be one outAs labeled end traversal");
                    this.endPipeline = p1;
                    this.endPipelineStartAs = GremlinHelper.getStart(p1).getAs();
                } else {
                    final List<Traversal> list = this.internalPipelines.getOrDefault(start, new ArrayList<>());
                    this.internalPipelines.put(start, list);
                    list.add(p1);
                }
            }
        }
        if (null == this.endPipeline)
            throw new IllegalStateException("One of the match traversals must be an end traversal");
    }

    protected Holder<E> processNextStart() {
        while (true) {
            if (this.endPipeline.hasNext()) {
                final Holder<E> holder = (Holder<E>) GremlinHelper.getEnd(this.endPipeline).next();
                if (doPredicates(this.outAs, holder)) {
                    return holder;
                }
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

        for (final Traversal traversal : this.internalPipelines.get(as)) {
            traversal.addStarts(new SingleIterator<>(holder));
            final Step<?, ?> endStep = GremlinHelper.getEnd(traversal);
            while (endStep.hasNext()) {
                final Holder temp = endStep.next();
                doMatch(endStep.getAs(), temp);
            }
        }
    }

    private boolean doPredicates(final String as, final Holder holder) {
        if (this.predicatePipelines.containsKey(as)) {
            for (final Traversal traversal : this.predicatePipelines.get(as)) {
                traversal.addStarts(new SingleIterator<>(holder));
                if (!GremlinHelper.hasNextIteration(traversal))
                    return false;
            }
        }
        return true;
    }

}
