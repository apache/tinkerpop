package com.tinkerpop.gremlin.pipes.util.optimizers;

import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.pipes.filter.SimplePathPipe;
import com.tinkerpop.gremlin.pipes.map.BackPipe;
import com.tinkerpop.gremlin.pipes.map.GraphQueryPipe;
import com.tinkerpop.gremlin.pipes.map.MatchPipe;
import com.tinkerpop.gremlin.pipes.map.PathPipe;
import com.tinkerpop.gremlin.pipes.map.SelectPipe;
import com.tinkerpop.gremlin.pipes.sideeffect.LinkPipe;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderOptimizer implements Optimizer.FinalOptimizer {

    public Pipeline optimize(final Pipeline pipeline) {
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
                        || p instanceof MatchPipe
                        || p instanceof LinkPipe).findFirst().isPresent();
    }
}
