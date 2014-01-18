package com.tinkerpop.gremlin.pipes.util.optimizers;

import com.tinkerpop.gremlin.pipes.map.MatchPipe;
import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.pipes.filter.SimplePathPipe;
import com.tinkerpop.gremlin.pipes.map.BackPipe;
import com.tinkerpop.gremlin.pipes.map.GraphQueryPipe;
import com.tinkerpop.gremlin.pipes.map.PathPipe;
import com.tinkerpop.gremlin.pipes.map.SelectPipe;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderOptimizer implements Optimizer {

    public <S, E> Pipeline<S, E> optimize(final Pipeline<S, E> pipeline) {
        final boolean trackPaths = this.trackPaths(pipeline);
        pipeline.getPipes().forEach(p -> {
            if (p instanceof GraphQueryPipe)
                ((GraphQueryPipe) p).generateHolderIterator(trackPaths);
        });
        return pipeline;
    }

    public <S, E> boolean trackPaths(final Pipeline<S, E> pipeline) {
        return pipeline.getPipes().stream().filter(p ->
                p instanceof PathPipe
                        || p instanceof BackPipe
                        || p instanceof SelectPipe
                        || p instanceof SimplePathPipe
                        || p instanceof MatchPipe).findFirst().isPresent();
    }

    public Rate getOptimizationRate() {
        return Rate.FINAL_COMPILE_TIME;
    }
}
