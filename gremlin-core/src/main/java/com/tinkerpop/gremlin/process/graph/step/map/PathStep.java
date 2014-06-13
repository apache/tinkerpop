package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.util.function.SFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathStep<S> extends MapStep<S, Path> implements PathConsumer {

    public FunctionRing functionRing;

    public PathStep(final Traversal traversal, final SFunction... pathFunctions) {
        super(traversal);
        this.functionRing = new FunctionRing(pathFunctions);
        this.setFunction(traverser -> {
            final Path path = new Path();
            traverser.getPath().forEach((a, b) -> path.add(a, this.functionRing.next().apply(b)));
            return path;
        });
    }
}
