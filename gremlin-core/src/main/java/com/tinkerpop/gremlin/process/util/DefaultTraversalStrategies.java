package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.traversers.TraverserGeneratorFactory;
import com.tinkerpop.gremlin.process.traversers.util.DefaultTraverserGeneratorFactory;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversalStrategies implements TraversalStrategies {

    protected final List<TraversalStrategy> traversalStrategies = new ArrayList<>();
    protected TraverserGeneratorFactory traverserGeneratorFactory = DefaultTraverserGeneratorFactory.instance();

    public DefaultTraversalStrategies() {

    }

    @Override
    public List<TraversalStrategy> toList() {
        return new ArrayList<>(this.traversalStrategies);
    }

    @Override
    public void register(final TraversalStrategy traversalStrategy) {
        if (!this.traversalStrategies.contains(traversalStrategy)) {
            this.traversalStrategies.add(traversalStrategy);
            TraversalStrategy.sortStrategies(this.traversalStrategies);
        }
    }

    @Override
    public void unregister(final Class<? extends TraversalStrategy> traversalStrategyClass) {
        this.traversalStrategies.stream().filter(c -> traversalStrategyClass.isAssignableFrom(c.getClass()))
                .collect(Collectors.toList())
                .forEach(this.traversalStrategies::remove);
        TraversalStrategy.sortStrategies(this.traversalStrategies);
    }

    @Override
    public void apply(final Traversal traversal, final TraversalEngine engine) {
        this.traversalStrategies.forEach(ts -> ts.apply(traversal, engine));
    }

    @Override
    public void clear() {
        this.traversalStrategies.clear();
    }

    @Override
    public String toString() {
        return StringFactory.traversalStrategiesString(this);
    }

    @Override
    public void registerTraverserGeneratorFactory(final TraverserGeneratorFactory traverserGeneratorFactory) {
        this.traverserGeneratorFactory = traverserGeneratorFactory;
    }

    @Override
    public TraverserGenerator getTraverserGenerator(final Traversal traversal, final TraversalEngine engine) {
        return this.traverserGeneratorFactory.getTraverserGenerator(traversal);
    }
}
