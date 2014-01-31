package com.tinkerpop.gremlin.util.optimizers;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.filter.HasPipe;
import com.tinkerpop.gremlin.oltp.filter.IntervalPipe;
import com.tinkerpop.gremlin.oltp.filter.RangePipe;
import com.tinkerpop.gremlin.oltp.map.EdgeVertexPipe;
import com.tinkerpop.gremlin.oltp.map.IdentityPipe;
import com.tinkerpop.gremlin.oltp.map.VertexQueryPipe;
import com.tinkerpop.gremlin.util.GremlinHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexQueryOptimizer implements Optimizer.StepOptimizer {

    private static final List<Class> COMPILED_PIPES = new ArrayList<Class>(
            Arrays.asList(IdentityPipe.class,
                    HasPipe.class,
                    IntervalPipe.class,
                    RangePipe.class,
                    EdgeVertexPipe.class));

    public boolean optimize(final Pipeline pipeline, final Pipe pipe) {
        if (!COMPILED_PIPES.stream().filter(c -> c.isAssignableFrom(pipe.getClass())).findFirst().isPresent())
            return true;
        else {
            if (GremlinHelper.isLabeled(pipe))
                return true;
        }

        VertexQueryPipe vertexQueryPipe = null;
        for (int i = pipeline.getPipes().size() - 1; i >= 0; i--) {
            final Pipe tempPipe = (Pipe) pipeline.getPipes().get(i);
            if (tempPipe instanceof VertexQueryPipe) {
                vertexQueryPipe = (VertexQueryPipe) tempPipe;
                break;
            } else if (!COMPILED_PIPES.stream().filter(c -> c.isAssignableFrom(tempPipe.getClass())).findFirst().isPresent())
                break;
        }

        if (null != vertexQueryPipe && !GremlinHelper.isLabeled(vertexQueryPipe)) {
            if (pipe instanceof EdgeVertexPipe) {
                if (((EdgeVertexPipe) pipe).direction.equals(vertexQueryPipe.queryBuilder.direction.opposite()))
                    vertexQueryPipe.returnClass = Vertex.class;
                else
                    return true;
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
