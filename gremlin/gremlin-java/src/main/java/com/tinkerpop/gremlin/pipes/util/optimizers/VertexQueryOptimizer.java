package com.tinkerpop.gremlin.pipes.util.optimizers;

import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.pipes.filter.RangePipe;
import com.tinkerpop.gremlin.pipes.map.VertexVertexPipe;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexQueryOptimizer implements Optimizer {

    public <S, E> Pipeline<S, E> optimize(final Pipeline<S, E> pipeline) {
        final List<Pipe<?, ?>> pipes = pipeline.getPipes();
        for (int i = 0; i < pipes.size(); i++) {
            if (pipes.get(i) instanceof RangePipe) {
                if (pipes.get(i - 1) instanceof VertexVertexPipe) {
                    // ((VertexVertexPipe)pipes.get(i-1)).queryBuilder
                }
            }
        }
        return pipeline;
    }

    public Rate getOptimizationRate() {
        return Rate.COMPILE_TIME;
    }
}
