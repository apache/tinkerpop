package com.tinkerpop.gremlin.process.traversers;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.TraverserGenerator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathTraverserGenerator implements TraverserGenerator {

    public <S> Traverser.Admin<S> generate(final S start, final Step<S, ?> startStep, final long initialBulk) {
        final PathTraverser<S> traverser = new PathTraverser<>(start, startStep);
        traverser.setBulk(initialBulk);
        return traverser;
    }
}