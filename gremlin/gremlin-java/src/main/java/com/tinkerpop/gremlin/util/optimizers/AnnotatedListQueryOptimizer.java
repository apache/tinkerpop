package com.tinkerpop.gremlin.util.optimizers;

import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.filter.HasPipe;
import com.tinkerpop.gremlin.oltp.filter.IntervalPipe;
import com.tinkerpop.gremlin.oltp.filter.RangePipe;
import com.tinkerpop.gremlin.oltp.map.AnnotatedListQueryPipe;
import com.tinkerpop.gremlin.oltp.map.AnnotatedValuePipe;
import com.tinkerpop.gremlin.oltp.map.IdentityPipe;
import com.tinkerpop.gremlin.util.GremlinHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedListQueryOptimizer implements Optimizer.StepOptimizer {

    private static final List<Class> COMPILED_PIPES = new ArrayList<Class>(
            Arrays.asList(IdentityPipe.class,
                    HasPipe.class,
                    IntervalPipe.class,
                    RangePipe.class,
                    AnnotatedValuePipe.class));

    public boolean optimize(final Pipeline pipeline, final Pipe pipe) {
        if (!COMPILED_PIPES.stream().filter(c -> c.isAssignableFrom(pipe.getClass())).findFirst().isPresent())
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
            } else if (!COMPILED_PIPES.stream().filter(c -> c.isAssignableFrom(tempPipe.getClass())).findFirst().isPresent())
                break;
        }

        if (null != annotatedListQueryPipe && !GremlinHelper.isLabeled(annotatedListQueryPipe)) {
            if (pipe instanceof AnnotatedValuePipe) {
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
            } else {
                throw new IllegalStateException("This pipe should not be accessible via this optimizer: " + pipe.getClass());
            }
            return false;
        }

        return true;
    }
}