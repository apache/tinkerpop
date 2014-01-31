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

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedListQueryOptimizer implements Optimizer.StepOptimizer {

    public boolean optimize(final Pipeline pipeline, final Pipe pipe) {
        if (!(pipe instanceof HasPipe || pipe instanceof IntervalPipe || pipe instanceof RangePipe || pipe instanceof AnnotatedValuePipe))
            return true;
        else {
            if (GremlinHelper.isLabeled(pipe))
                return true;
        }

        AnnotatedListQueryPipe annotatedListQueryPipe = null;
        for (int i = pipeline.getPipes().size() - 1; i >= 0; i--) {
            if (pipeline.getPipes().get(i) instanceof AnnotatedListQueryPipe) {
                annotatedListQueryPipe = (AnnotatedListQueryPipe) pipeline.getPipes().get(i);
                break;
            } else if (!(pipeline.getPipes().get(i) instanceof IdentityPipe
                    || pipeline.getPipes().get(i) instanceof HasPipe
                    || pipeline.getPipes().get(i) instanceof IntervalPipe
                    || pipeline.getPipes().get(i) instanceof RangePipe))
                break;
        }

        if (null != annotatedListQueryPipe && !GremlinHelper.isLabeled(annotatedListQueryPipe)) {
            if (pipe instanceof AnnotatedValuePipe) {
                annotatedListQueryPipe.returnAnnotatedValues = false;
                annotatedListQueryPipe.generateFunction();
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
            annotatedListQueryPipe.generateFunction();
            return false;
        }

        return true;
    }
}