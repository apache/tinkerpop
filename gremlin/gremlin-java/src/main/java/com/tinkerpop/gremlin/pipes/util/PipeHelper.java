package com.tinkerpop.gremlin.pipes.util;

import com.tinkerpop.gremlin.pipes.Pipe;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PipeHelper {

    public static boolean hasNextIteration(final Pipe pipe) {
        if (pipe.hasNext()) {
            while (pipe.hasNext()) {
                pipe.next();
            }
            return true;
        } else {
            return false;
        }
    }

    public static <T> List<Holder<T>> toList(final Pipe<?, T> pipe) {
        final List<Holder<T>> result = new ArrayList<>();
        while (pipe.hasNext()) {
            result.add(pipe.next());
        }
        return result;
    }
}
