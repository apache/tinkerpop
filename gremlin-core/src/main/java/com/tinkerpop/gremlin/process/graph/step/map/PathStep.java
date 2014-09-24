package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.util.FunctionRing;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathStep<S> extends MapStep<S, Path> implements PathConsumer {

    public FunctionRing functionRing;

    public PathStep(final Traversal traversal, final Function... pathFunctions) {
        super(traversal);
        this.functionRing = (pathFunctions.length == 0) ? null : new FunctionRing(pathFunctions);
        this.setFunction(traverser -> {
            final Path path = new Path();
            if (null == this.functionRing)
                path.add(traverser.getPath());
            else {
                traverser.getPath().forEach((a, b) -> path.add(a, this.functionRing.next().apply(b)));
                this.functionRing.reset();
            }
            return path;
        });
    }

    @Override
    public void reset() {
        super.reset();
        this.functionRing.reset();
    }
}
