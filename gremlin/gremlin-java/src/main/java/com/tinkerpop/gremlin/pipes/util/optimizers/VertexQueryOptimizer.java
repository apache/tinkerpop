package com.tinkerpop.gremlin.pipes.util.optimizers;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.pipes.filter.HasPipe;
import com.tinkerpop.gremlin.pipes.filter.IntervalPipe;
import com.tinkerpop.gremlin.pipes.filter.RangePipe;
import com.tinkerpop.gremlin.pipes.map.EdgeVertexPipe;
import com.tinkerpop.gremlin.pipes.map.IdentityPipe;
import com.tinkerpop.gremlin.pipes.map.VertexQueryPipe;
import com.tinkerpop.gremlin.pipes.util.GremlinHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexQueryOptimizer implements Optimizer.StepOptimizer, Optimizer {

    public boolean optimize(final Pipeline pipeline, final Pipe pipe) {
        if (!(pipe instanceof HasPipe || pipe instanceof IntervalPipe || pipe instanceof EdgeVertexPipe || pipe instanceof RangePipe))
            return true;
        else {
            if (GremlinHelper.isLabeled(pipe))
                return true;
        }

        VertexQueryPipe vertexQueryPipe = null;
        for (int i = pipeline.getPipes().size() - 1; i >= 0; i--) {
            if (pipeline.getPipes().get(i) instanceof VertexQueryPipe) {
                vertexQueryPipe = (VertexQueryPipe) pipeline.getPipes().get(i);
                break;
            } else if (!(pipeline.getPipes().get(i) instanceof IdentityPipe
                    || pipeline.getPipes().get(i) instanceof HasPipe
                    || pipeline.getPipes().get(i) instanceof IntervalPipe
                    || pipeline.getPipes().get(i) instanceof RangePipe))
                break;
        }

        if (null != vertexQueryPipe && !GremlinHelper.isLabeled(vertexQueryPipe)) {
            if (pipe instanceof EdgeVertexPipe) {
                vertexQueryPipe.returnClass = Vertex.class;
            } else if (pipe instanceof HasPipe) {
                final HasPipe hasPipe = (HasPipe) pipe;
                if (!vertexQueryPipe.returnClass.equals(Vertex.class)) {
                    vertexQueryPipe.queryBuilder.has(hasPipe.hasContainer.key, hasPipe.hasContainer.predicate, hasPipe.hasContainer.value);
                } else
                    return true;
            } else if (pipe instanceof IntervalPipe) {
                final IntervalPipe intervalPipe = (IntervalPipe) pipe;
                vertexQueryPipe.queryBuilder.has(intervalPipe.startContainer.key, intervalPipe.startContainer.predicate, intervalPipe.startContainer.value);
                vertexQueryPipe.queryBuilder.has(intervalPipe.endContainer.key, intervalPipe.endContainer.predicate, intervalPipe.endContainer.value);
            } else if (pipe instanceof RangePipe) {
                final RangePipe rangePipe = (RangePipe) pipe;
                vertexQueryPipe.low = rangePipe.low;
                vertexQueryPipe.high = rangePipe.high;
            } else {
                throw new IllegalStateException("This pipe should not be accessible via this optimizer: " + pipe.getClass());
            }
            vertexQueryPipe.generateFunction();
            return false;
        }

        return true;
    }
}
