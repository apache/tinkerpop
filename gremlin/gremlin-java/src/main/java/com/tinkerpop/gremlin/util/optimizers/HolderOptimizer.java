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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderOptimizer implements Optimizer.FinalOptimizer {

    private static final List<Class> PATH_PIPES = new ArrayList<Class>(
            Arrays.asList(
                    PathPipe.class,
                    BackPipe.class,
                    SelectPipe.class,
                    SimplePathPipe.class,
                    MatchPipe.class,
                    LinkPipe.class));

    public void optimize(final Pipeline pipeline) {
        final boolean trackPaths = HolderOptimizer.trackPaths(pipeline);
        pipeline.getPipes().forEach(pipe -> {
            if (pipe instanceof GraphQueryPipe)
                ((GraphQueryPipe) pipe).generateHolderIterator(trackPaths);
        });
    }

    public static <S, E> boolean trackPaths(final Pipeline<S, E> pipeline) {
        return pipeline.getPipes().stream()
                .filter(pipe -> PATH_PIPES.stream().filter(c -> c.isAssignableFrom(pipe.getClass())).findFirst().isPresent())
                .findFirst()
                .isPresent();
    }

    public static <S, E> void doPathTracking(final Pipeline<S, E> pipeline) {
        pipeline.getPipes().forEach(pipe -> {
            if (pipe instanceof GraphQueryPipe)
                ((GraphQueryPipe) pipe).generateHolderIterator(true);
        });
    }
}
