package com.tinkerpop.gremlin.pipes.util;

import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.SimpleHolder;

import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinHelper {

    public static <S, E> Pipe<S, E> getAs(final String as, final Pipeline<?, ?> pipeline) {
        return (Pipe) pipeline.getPipes().stream()
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
                    return Holder.NO_FUTURE;
                else {
                    return ((Pipe) pipeline.getPipes().get(i + 1)).getAs();
                }
            }
        }
        return SimpleHolder.NO_FUTURE;
    }

    public static <S, E> Pipe<S, ?> getStart(final Pipeline<S, E> pipeline) {
        return (Pipe) pipeline.getPipes().get(0);
    }

    public static <S, E> Pipe<?, E> getEnd(final Pipeline<S, E> pipeline) {
        return (Pipe) pipeline.getPipes().get(pipeline.getPipes().size() - 1);
    }

    public static void chainPipes(final Pipeline pipeline) {
        final List<Pipe> pipes = pipeline.getPipes();
        if (pipes.size() > 0) {
            for (int i = 1; i < pipes.size(); i++) {
                pipes.get(i).addStarts(pipes.get(i - 1));
            }
        }
    }

    public static boolean hasNextIteration(final Iterator iterator) {
        if (iterator.hasNext()) {
            while (iterator.hasNext()) {
                iterator.next();
            }
            return true;
        } else {
            return false;
        }
    }
}
