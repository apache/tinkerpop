package com.tinkerpop.gremlin.process.traverser.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.traverser.PathTraverserGenerator;
import com.tinkerpop.gremlin.process.traverser.SimpleTraverserGenerator;
import com.tinkerpop.gremlin.process.traverser.TraverserGeneratorFactory;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraverserGeneratorFactory implements TraverserGeneratorFactory {

    private static DefaultTraverserGeneratorFactory INSTANCE = new DefaultTraverserGeneratorFactory();
    private static final PathTraverserGenerator PATH_TRAVERSER_GENERATOR = PathTraverserGenerator.instance();
    private static final SimpleTraverserGenerator SIMPLE_TRAVERSER_GENERATOR = SimpleTraverserGenerator.instance();

    public static DefaultTraverserGeneratorFactory instance() {
        return INSTANCE;
    }

    private DefaultTraverserGeneratorFactory() {
    }

    public TraverserGenerator getTraverserGenerator(final Traversal traversal) {
        return this.getRequirements(traversal).contains(TraverserRequirements.UNIQUE_PATH) ?
                PATH_TRAVERSER_GENERATOR :
                SIMPLE_TRAVERSER_GENERATOR;
    }

    public Set<TraverserRequirements> getRequirements(final Traversal traversal) {
        return TraversalHelper.trackPaths(traversal) ?
                Collections.singleton(TraverserRequirements.UNIQUE_PATH) :
                Collections.emptySet();
    }
}
