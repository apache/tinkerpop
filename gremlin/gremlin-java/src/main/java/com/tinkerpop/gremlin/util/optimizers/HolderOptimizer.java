package com.tinkerpop.gremlin.util.optimizers;

import com.tinkerpop.gremlin.Optimizer;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.filter.SimplePathPipe;
import com.tinkerpop.gremlin.oltp.map.BackPipe;
import com.tinkerpop.gremlin.oltp.map.GraphQueryPipe;
import com.tinkerpop.gremlin.oltp.map.MatchPipe;
import com.tinkerpop.gremlin.oltp.map.PathPipe;
import com.tinkerpop.gremlin.oltp.map.SelectPipe;
import com.tinkerpop.gremlin.oltp.sideeffect.LinkPipe;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderOptimizer implements Optimizer.FinalOptimizer {

    public Pipeline optimize(final Pipeline pipeline) {
        final boolean trackPaths = HolderOptimizer.trackPaths(pipeline);
        pipeline.getPipes().forEach(pipe -> {
            if (pipe instanceof GraphQueryPipe)
                ((GraphQueryPipe) pipe).generateHolderIterator(trackPaths);
        });
        return pipeline;
    }

    public static <S, E> boolean trackPaths(final Pipeline<S, E> pipeline) {
        return pipeline.getPipes().stream().filter(pipe ->
                pipe instanceof PathPipe
                        || pipe instanceof BackPipe
                        || pipe instanceof SelectPipe
                        || pipe instanceof SimplePathPipe
                        || pipe instanceof MatchPipe
                        || pipe instanceof LinkPipe).findFirst().isPresent();
    }

    public static <S, E> void doPathTracking(final Pipeline<S, E> pipeline) {
        pipeline.getPipes().forEach(pipe -> {
            if (pipe instanceof GraphQueryPipe)
                ((GraphQueryPipe) pipe).generateHolderIterator(true);
        });
    }
}
