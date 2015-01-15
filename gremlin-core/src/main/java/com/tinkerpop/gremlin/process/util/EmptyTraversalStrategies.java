package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.traverser.TraverserGeneratorFactory;
import com.tinkerpop.gremlin.process.traverser.util.DefaultTraverserGeneratorFactory;

import java.util.Collections;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyTraversalStrategies implements TraversalStrategies {

    private static final EmptyTraversalStrategies INSTANCE = new EmptyTraversalStrategies();

    private EmptyTraversalStrategies() {
    }

    @Override
    public List<TraversalStrategy> toList() {
        return Collections.emptyList();
    }

    @Override
    public void applyStrategies(final Traversal traversal, final TraversalEngine engine) {

    }

    @Override
    public TraversalStrategies addStrategies(final TraversalStrategy... strategies) {
        return this;
    }

    @Override
    public TraversalStrategies removeStrategies(final Class<? extends TraversalStrategy>... strategyClasses) {
        return this;
    }

    @Override
    public TraversalStrategies clone() throws CloneNotSupportedException {
        return this;
    }

    @Override
    public TraverserGenerator getTraverserGenerator(final Traversal traversal) {
        return DefaultTraverserGeneratorFactory.instance().getTraverserGenerator(traversal);
    }

    @Override
    public void setTraverserGeneratorFactory(final TraverserGeneratorFactory traverserGeneratorFactory) {

    }

    public static EmptyTraversalStrategies instance() {
        return INSTANCE;
    }
}
