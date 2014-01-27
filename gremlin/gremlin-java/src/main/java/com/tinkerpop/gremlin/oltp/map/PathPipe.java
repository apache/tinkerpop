package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Path;
import com.tinkerpop.gremlin.Pipeline;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathPipe<S> extends MapPipe<S, Path> {

    public Function[] pathFunctions;
    public int currentFunction = 0;

    public PathPipe(final Pipeline pipeline, final Function... pathFunctions) {
        super(pipeline);
        this.pathFunctions = pathFunctions;
        if (null == this.pathFunctions || this.pathFunctions.length == 0)
            super.setFunction(Holder::getPath);
        else {
            super.setFunction(holder -> {
                final Path path = new Path();
                holder.getPath().forEach((a, b) -> {
                    path.add(a, this.pathFunctions[this.currentFunction].apply(b));
                    this.currentFunction = (this.currentFunction + 1) % this.pathFunctions.length;
                });
                return path;
            });
        }
    }
}
