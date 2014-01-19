package com.tinkerpop.gremlin.pipes.util.optimizers;

import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.pipes.filter.HasPipe;
import com.tinkerpop.gremlin.pipes.filter.IntervalPipe;
import com.tinkerpop.gremlin.pipes.map.IdentityPipe;
import com.tinkerpop.gremlin.pipes.map.VertexEdgePipe;
import com.tinkerpop.gremlin.pipes.util.GremlinHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexQueryOptimizer implements Optimizer {

    public <S, E> Pipeline<S, E> optimize(final Pipeline<S, E> pipeline) {
        final Pipe lastPipe = GremlinHelper.getEnd(pipeline);
        if (!(lastPipe instanceof HasPipe || lastPipe instanceof IntervalPipe))
            return pipeline;

        VertexEdgePipe vertexEdgePipe = null;
        for (int i = pipeline.getPipes().size() - 1; i >= 0; i--) {
            if (pipeline.getPipes().get(i) instanceof VertexEdgePipe) {
                vertexEdgePipe = (VertexEdgePipe) pipeline.getPipes().get(i);
                break;
            } else if (!(pipeline.getPipes().get(i) instanceof IdentityPipe
                    || pipeline.getPipes().get(i) instanceof HasPipe
                    || pipeline.getPipes().get(i) instanceof IntervalPipe))
                break;
        }

        if (null != vertexEdgePipe) {
            if (lastPipe instanceof HasPipe) {
                vertexEdgePipe.queryBuilder.has(((HasPipe) lastPipe).hasContainer.key, ((HasPipe) lastPipe).hasContainer.predicate, ((HasPipe) lastPipe).hasContainer.value);
            } else {
                vertexEdgePipe.queryBuilder.has(((IntervalPipe) lastPipe).startContainer.key, ((IntervalPipe) lastPipe).startContainer.predicate, ((IntervalPipe) lastPipe).startContainer.value);
                vertexEdgePipe.queryBuilder.has(((IntervalPipe) lastPipe).endContainer.key, ((IntervalPipe) lastPipe).endContainer.predicate, ((IntervalPipe) lastPipe).endContainer.value);
            }
            pipeline.getPipes().remove(lastPipe);
        }

        return pipeline;
    }

    public Rate getOptimizationRate() {
        return Rate.STEP_COMPILE_TIME;
    }
}
