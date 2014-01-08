package com.tinkerpop.gremlin.pipes.util;

import com.tinkerpop.gremlin.pipes.Pipe;
import com.tinkerpop.gremlin.pipes.Pipeline;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PipelineHelper {

    public static <S, E> Pipe<S, E> getAs(final String name, final Pipeline<?, ?> pipeline) {
        return (Pipe<S, E>) pipeline.getPipes().stream()
                .filter(p -> name.equals(p.getAs()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("The provided name does not exist: " + name));
    }

    public static int getAsIndex(final String name, final Pipeline<?, ?> pipeline) {
        for (int i = 0; i < pipeline.getPipes().size(); i++) {
            if (pipeline.getPipes().get(i).getAs().equals(name))
                return i;
        }
        return -1;
    }

    public static Optional<String> getNextPipeLabel(final Pipeline pipeline, final Pipe pipe) {
        for (int i = 0; i < pipeline.getPipes().size(); i++) {
            if (pipeline.getPipes().get(i) == pipe) {
                if (i == pipeline.getPipes().size() - 1)
                    return Optional.empty();
                else
                    return Optional.of(((Pipe) pipeline.getPipes().get(i + 1)).getAs());
            }
        }
        return Optional.empty();
    }

    public static boolean asExists(final String name, final Pipeline<?, ?> pipeline) {
        return pipeline.getPipes().stream()
                .filter(p -> name.equals(p.getAs()))
                .findFirst().isPresent();
    }

    public static <S, E> Pipe<S, ?> getStart(final Pipeline<S, E> pipeline) {
        return (Pipe) pipeline.getPipes().get(0);
    }

    public static <S, E> Pipe<?, E> getEnd(final Pipeline<S, E> pipeline) {
        return (Pipe) pipeline.getPipes().get(pipeline.getPipes().size() - 1);
    }
}
