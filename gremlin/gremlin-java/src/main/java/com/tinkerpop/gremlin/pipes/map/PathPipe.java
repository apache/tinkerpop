package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.MapPipe;
import com.tinkerpop.gremlin.Path;
import com.tinkerpop.gremlin.Pipeline;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathPipe<S> extends MapPipe<S, Path> {

    public Function[] pathFunctions;

    public PathPipe(final Pipeline pipeline, final Function... pathFunctions) {
        super(pipeline);
        this.pathFunctions = pathFunctions;
        if (null == this.pathFunctions || this.pathFunctions.length == 0)
            super.setFunction(Holder::getPath);
        else {
            final AtomicInteger nextFunction = new AtomicInteger(0);
            super.setFunction(o -> {
                final Path path = new Path();
                o.getPath().forEach((a, b) -> {
                    path.add(a, pathFunctions[nextFunction.get()].apply(b));
                    nextFunction.set((nextFunction.get() + 1) % pathFunctions.length);
                });
                return path;
            });
        }
    }
}
