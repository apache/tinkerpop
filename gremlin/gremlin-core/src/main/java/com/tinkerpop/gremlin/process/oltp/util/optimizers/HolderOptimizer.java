package com.tinkerpop.gremlin.process.oltp.util.optimizers;

import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.filter.SimplePathPipe;
import com.tinkerpop.gremlin.process.oltp.map.BackPipe;
import com.tinkerpop.gremlin.process.oltp.map.GraphQueryPipe;
import com.tinkerpop.gremlin.process.oltp.map.MatchPipe;
import com.tinkerpop.gremlin.process.oltp.map.PathPipe;
import com.tinkerpop.gremlin.process.oltp.map.SelectPipe;
import com.tinkerpop.gremlin.process.oltp.sideeffect.LinkPipe;

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

    public void optimize(final Traversal pipeline) {
        final boolean trackPaths = HolderOptimizer.trackPaths(pipeline);
        pipeline.getPipes().forEach(pipe -> {
            if (pipe instanceof GraphQueryPipe)
                ((GraphQueryPipe) pipe).generateHolderIterator(trackPaths);
        });
    }

    public static <S, E> boolean trackPaths(final Traversal<S, E> pipeline) {
        return pipeline.getPipes().stream()
                .filter(pipe -> PATH_PIPES.stream().filter(c -> c.isAssignableFrom(pipe.getClass())).findFirst().isPresent())
                .findFirst()
                .isPresent();
    }

    public static <S, E> void doPathTracking(final Traversal<S, E> pipeline) {
        pipeline.getPipes().forEach(pipe -> {
            if (pipe instanceof GraphQueryPipe)
                ((GraphQueryPipe) pipe).generateHolderIterator(true);
        });
    }
}
