package com.tinkerpop.gremlin.process.traversers.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traversers.PathTraverserGenerator;
import com.tinkerpop.gremlin.process.traversers.SimpleTraverserGenerator;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.traversers.TraverserGeneratorFactory;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraverserGeneratorFactory implements TraverserGeneratorFactory {

    private static DefaultTraverserGeneratorFactory INSTANCE = new DefaultTraverserGeneratorFactory();

    public static DefaultTraverserGeneratorFactory instance() {
        return INSTANCE;
    }

    private DefaultTraverserGeneratorFactory() {
    }

    public TraverserGenerator getTraverserGenerator(final Traversal traversal) {
        return TraversalHelper.trackPaths(traversal) ?
                new PathTraverserGenerator() :
                new SimpleTraverserGenerator();
    }
}
