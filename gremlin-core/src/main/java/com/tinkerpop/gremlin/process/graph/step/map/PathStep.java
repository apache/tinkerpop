package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.MutablePath;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PathStep<S> extends MapStep<S, Path> implements PathConsumer {

    private final FunctionRing functionRing;

    public PathStep(final Traversal traversal, final Function... pathFunctions) {
        super(traversal);
        this.functionRing = (pathFunctions.length == 0) ? null : new FunctionRing(pathFunctions);
        this.setFunction(traverser -> {
            if (null == this.functionRing)
                return traverser.path();
            else {
                final Path path = MutablePath.make();
                traverser.path().forEach((labels, object) -> path.extend(labels, this.functionRing.next().apply(object)));
                this.functionRing.reset();
                return path;
            }
        });
    }

    @Override
    public void reset() {
        super.reset();
        this.functionRing.reset();
    }
}
