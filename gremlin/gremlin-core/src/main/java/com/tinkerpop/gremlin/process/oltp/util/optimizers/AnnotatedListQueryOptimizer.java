package com.tinkerpop.gremlin.process.oltp.util.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Pipe;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.filter.HasPipe;
import com.tinkerpop.gremlin.process.oltp.filter.IntervalPipe;
import com.tinkerpop.gremlin.process.oltp.filter.RangePipe;
import com.tinkerpop.gremlin.process.oltp.map.AnnotatedListQueryPipe;
import com.tinkerpop.gremlin.process.oltp.map.IdentityPipe;
import com.tinkerpop.gremlin.process.oltp.map.ValuePipe;
import com.tinkerpop.gremlin.process.oltp.util.GremlinHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedListQueryOptimizer implements Optimizer.StepOptimizer {

    private static final List<Class> PIPES_TO_FOLD = new ArrayList<Class>(
            Arrays.asList(
                    IdentityPipe.class,
                    HasPipe.class,
                    IntervalPipe.class,
                    RangePipe.class,
                    ValuePipe.class));

    public boolean optimize(final Traversal pipeline, final Pipe pipe) {
        if (!PIPES_TO_FOLD.stream().filter(c -> c.isAssignableFrom(pipe.getClass())).findFirst().isPresent())
            return true;
        else {
            if (GremlinHelper.isLabeled(pipe))
                return true;
        }

        AnnotatedListQueryPipe annotatedListQueryPipe = null;
        for (int i = pipeline.getPipes().size() - 1; i >= 0; i--) {
            final Pipe tempPipe = (Pipe) pipeline.getPipes().get(i);
            if (tempPipe instanceof AnnotatedListQueryPipe) {
                annotatedListQueryPipe = (AnnotatedListQueryPipe) tempPipe;
                break;
            } else if (!PIPES_TO_FOLD.stream().filter(c -> c.isAssignableFrom(tempPipe.getClass())).findFirst().isPresent())
                break;
        }

        if (null != annotatedListQueryPipe && !GremlinHelper.isLabeled(annotatedListQueryPipe)) {
            if (pipe instanceof ValuePipe) {
                annotatedListQueryPipe.generateFunction(false);
            } else if (pipe instanceof HasPipe) {
                final HasPipe hasPipe = (HasPipe) pipe;
                annotatedListQueryPipe.queryBuilder.has(hasPipe.hasContainer.key, hasPipe.hasContainer.predicate, hasPipe.hasContainer.value);
            } else if (pipe instanceof IntervalPipe) {
                final IntervalPipe intervalPipe = (IntervalPipe) pipe;
                annotatedListQueryPipe.queryBuilder.has(intervalPipe.startContainer.key, intervalPipe.startContainer.predicate, intervalPipe.startContainer.value);
                annotatedListQueryPipe.queryBuilder.has(intervalPipe.endContainer.key, intervalPipe.endContainer.predicate, intervalPipe.endContainer.value);
            } else if (pipe instanceof RangePipe) {
                final RangePipe rangePipe = (RangePipe) pipe;
                annotatedListQueryPipe.low = rangePipe.low;
                annotatedListQueryPipe.high = rangePipe.high;
            }
            return false;
        }

        return true;
    }
}