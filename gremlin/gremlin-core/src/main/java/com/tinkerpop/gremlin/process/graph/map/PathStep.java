package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.PathConsumer;
import com.tinkerpop.gremlin.process.util.FunctionRing;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathStep<S> extends MapStep<S, Path> implements PathConsumer {

    public FunctionRing functionRing;

    public PathStep(final Traversal traversal, final Function... pathFunctions) {
        super(traversal);
        this.functionRing = new FunctionRing(pathFunctions);
        this.setFunction(holder -> {
            final Path path = new Path();
            holder.getPath().forEach((a, b) -> path.add(a, this.functionRing.next().apply(b)));
            return path;
        });
    }
}
