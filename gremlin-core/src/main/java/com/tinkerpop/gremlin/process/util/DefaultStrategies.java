package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultStrategies implements Traversal.Strategies {

    private List<TraversalStrategy> traversalStrategies = new ArrayList<>();
    protected Traversal traversal;
    private boolean complete = false;

    public DefaultStrategies(final Traversal traversal) {
        this.traversal = traversal;
    }

    @Override
    public List<TraversalStrategy> toList() {
        return new ArrayList<>(this.traversalStrategies);
    }

    @Override
    public void register(final TraversalStrategy traversalStrategy) {
        this.traversalStrategies.add(traversalStrategy);
        // TODO: make this a LinkedHashSet so repeats are not allowed? Or check for repeats first?
    }

    @Override
    public void unregister(final Class<? extends TraversalStrategy> optimizerClass) {
        this.traversalStrategies.stream().filter(c -> optimizerClass.isAssignableFrom(c.getClass()))
                .collect(Collectors.toList())
                .forEach(this.traversalStrategies::remove);
    }

    @Override
    public void apply() {
        if (!this.complete) {
            this.complete = true;
            Collections.sort(this.traversalStrategies);
            this.traversalStrategies.forEach(ts -> ts.apply(this.traversal));
        }
    }

    @Override
    public void clear() {
        this.traversalStrategies.clear();
    }

    @Override
    public boolean complete() {
        return this.complete;
    }

    @Override
    public String toString() {
        return StringFactory.traversalStrategiesString(this);
    }
}
