package com.tinkerpop.gremlin.process.traverser.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.traverser.PathTraverserGenerator;
import com.tinkerpop.gremlin.process.traverser.SimpleTraverserGenerator;
import com.tinkerpop.gremlin.process.traverser.TraverserGeneratorFactory;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraverserGeneratorFactory implements TraverserGeneratorFactory {

    private static DefaultTraverserGeneratorFactory INSTANCE = new DefaultTraverserGeneratorFactory();
    private static final PathTraverserGenerator PATH_TRAVERSER_GENERATOR = new PathTraverserGenerator();
    private static final SimpleTraverserGenerator SIMPLE_TRAVERSER_GENERATOR = new SimpleTraverserGenerator();

    public static DefaultTraverserGeneratorFactory instance() {
        return INSTANCE;
    }

    private DefaultTraverserGeneratorFactory() {
    }

    public TraverserGenerator getTraverserGenerator(final Traversal traversal) {
        return TraversalHelper.trackPaths(traversal) ?
                PATH_TRAVERSER_GENERATOR :
                SIMPLE_TRAVERSER_GENERATOR;
    }
}
