package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.traverser.TraverserGeneratorFactory;
import com.tinkerpop.gremlin.process.traverser.util.DefaultTraverserGeneratorFactory;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversalStrategies implements TraversalStrategies {

    protected List<TraversalStrategy> traversalStrategies = new ArrayList<>();
    protected TraverserGeneratorFactory traverserGeneratorFactory = DefaultTraverserGeneratorFactory.instance();

    @Override
    public TraversalStrategies addStrategies(final TraversalStrategy... strategies) {
        boolean added = false;
        for (final TraversalStrategy strategy : strategies) {
            if (!this.traversalStrategies.contains(strategy)) {
                this.traversalStrategies.add(strategy);
                added = true;
            }
        }
        if (added) TraversalStrategies.sortStrategies(this.traversalStrategies);
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TraversalStrategies removeStrategies(final Class<? extends TraversalStrategy>... strategyClasses) {
        boolean removed = false;
        for (final Class<? extends TraversalStrategy> strategyClass : strategyClasses) {
            final Optional<TraversalStrategy> strategy = this.traversalStrategies.stream().filter(s -> s.getClass().equals(strategyClass)).findAny();
            if (strategy.isPresent()) {
                this.traversalStrategies.remove(strategy.get());
                removed = true;
            }
        }
        if (removed) TraversalStrategies.sortStrategies(this.traversalStrategies);
        return this;
    }

    @Override
    public List<TraversalStrategy> toList() {
        return Collections.unmodifiableList(this.traversalStrategies);
    }

    @Override
    public void applyStrategies(final Traversal traversal, final TraversalEngine engine) {
        this.traversalStrategies.forEach(ts -> ts.apply(traversal.asAdmin(), engine));
    }

    @Override
    public TraverserGenerator getTraverserGenerator(final Traversal traversal) {
        return this.traverserGeneratorFactory.getTraverserGenerator(traversal);
    }

    @Override
    public void setTraverserGeneratorFactory(final TraverserGeneratorFactory traverserGeneratorFactory) {
        this.traverserGeneratorFactory = traverserGeneratorFactory;
    }

    @Override
    public DefaultTraversalStrategies clone() throws CloneNotSupportedException {
        final DefaultTraversalStrategies clone = (DefaultTraversalStrategies) super.clone();
        clone.traversalStrategies = new ArrayList<>();
        clone.traversalStrategies.addAll(this.traversalStrategies);
        // TraversalStrategies.sortStrategies(clone.traversalStrategies);
        return clone;
    }

    @Override
    public String toString() {
        return StringFactory.traversalStrategiesString(this);
    }
}
