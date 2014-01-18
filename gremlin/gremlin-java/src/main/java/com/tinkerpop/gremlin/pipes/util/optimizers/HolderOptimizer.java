package com.tinkerpop.gremlin.pipes.util.optimizers;

import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.pipes.map.BackPipe;
import com.tinkerpop.gremlin.pipes.map.PathPipe;
import com.tinkerpop.gremlin.pipes.map.SelectPipe;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderOptimizer implements Optimizer {

    public <S, E> Pipeline<S, E> optimize(final Pipeline<S, E> pipeline) {
        pipeline.getPipes().stream().filter(p ->
                p instanceof PathPipe
                        || p instanceof BackPipe
                        || p instanceof SelectPipe).findAny().ifPresent(p -> pipeline.trackPaths(true));
        return pipeline;
    }

    public Rate getOptimizationRate() {
        return Rate.COMPILE_TIME;
    }
}
