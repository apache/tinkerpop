package com.tinkerpop.gremlin.pipes.util;

import com.tinkerpop.gremlin.pipes.Pipe;
import com.tinkerpop.gremlin.pipes.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PipelineHelper {

    public static <S, E> Pipe getAs(final String name, final Pipeline<S, E> pipeline) {
        return pipeline.getPipes().stream().filter(p -> name.equals(p.getName())).findFirst().orElse(null);
    }

    public static <S, E> Pipe<S, ?> getStart(final Pipeline<S, E> pipeline) {
        return pipeline.getPipes().get(0);
    }

    public static <S, E> Pipe<?, E> getEnd(final Pipeline<S, E> pipeline) {
        return pipeline.getPipes().get(pipeline.getPipes().size() - 1);
    }
}
