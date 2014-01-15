package com.tinkerpop.gremlin.pipes.util;

import com.tinkerpop.gremlin.pipes.Pipe;
import com.tinkerpop.gremlin.SimpleHolder;
import com.tinkerpop.gremlin.pipes.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PipelineHelper {

    public static <S, E> Pipe<S, E> getAs(final String as, final Pipeline<?, ?> pipeline) {
        return (Pipe<S, E>) pipeline.getPipes().stream()
                .filter(p -> as.equals(p.getAs()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("The provided name does not exist: " + as));
    }

    public static boolean asExists(final String as, final Pipeline<?, ?> pipeline) {
        return pipeline.getPipes().stream()
                .filter(p -> as.equals(p.getAs()))
                .findFirst().isPresent();
    }

    public static String getNextPipeLabel(final Pipeline pipeline, final Pipe pipe) {
        for (int i = 0; i < pipeline.getPipes().size(); i++) {
            if (pipeline.getPipes().get(i) == pipe) {
                if (i == pipeline.getPipes().size() - 1)
                    return SimpleHolder.NONE;
                else {
                    return ((Pipe) pipeline.getPipes().get(i + 1)).getAs();
                }
            }
        }
        return SimpleHolder.NONE;
    }

    public static <S, E> Pipe<S, ?> getStart(final Pipeline<S, E> pipeline) {
        return (Pipe) pipeline.getPipes().get(0);
    }

    public static <S, E> Pipe<?, E> getEnd(final Pipeline<S, E> pipeline) {
        return (Pipe) pipeline.getPipes().get(pipeline.getPipes().size() - 1);
    }
}
