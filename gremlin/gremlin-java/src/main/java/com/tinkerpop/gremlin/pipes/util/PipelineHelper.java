package com.tinkerpop.gremlin.pipes.util;

import com.tinkerpop.gremlin.pipes.GremlinPipeline;
import com.tinkerpop.gremlin.pipes.Pipe;
import com.tinkerpop.gremlin.pipes.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PipelineHelper {

    public static Pipe getAs(final String name, final Pipeline<?,?> pipeline) {
        return pipeline.getPipes().stream()
                .filter(p -> name.equals(p.getAs()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("The provided name does not exist"));
    }

    public static boolean asExists(final String name, final Pipeline<?,?> pipeline) {
        return pipeline.getPipes().stream()
                .filter(p -> name.equals(p.getAs()))
                .findFirst().isPresent();
    }

    public static <S, E> Pipe<S, ?> getStart(final Pipeline<S, E> pipeline) {
        return pipeline.getPipes().get(0);
    }

    public static <S, E> Pipe<?, E> getEnd(final Pipeline<S, E> pipeline) {
        return pipeline.getPipes().get(pipeline.getPipes().size() - 1);
    }
}
