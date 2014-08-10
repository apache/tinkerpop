package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.TraversalStrategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversalStrategies implements TraversalStrategies {

    private final List<TraversalStrategy> traversalStrategies = new ArrayList<>();
    private final Traversal traversal;
    private boolean complete = false;

    public DefaultTraversalStrategies(final Traversal traversal) {
        this.traversal = traversal;
    }

    public void register(final TraversalStrategy traversalStrategy) {
        this.traversalStrategies.add(traversalStrategy);
        // TODO: make this a LinkedHashSet so repeats are not allowed? Or check for repeats first?
    }

    public void unregister(final Class<? extends TraversalStrategy> optimizerClass) {
        this.traversalStrategies.stream().filter(c -> optimizerClass.isAssignableFrom(c.getClass()))
                .collect(Collectors.toList())
                .forEach(this.traversalStrategies::remove);
    }

    public void apply() {
        this.complete = true;
        Collections.sort(this.traversalStrategies);
        this.traversalStrategies.forEach(ts -> ts.apply(this.traversal));
    }

    public void clear() {
        this.traversalStrategies.clear();
    }

    public boolean complete() {
        return this.complete;
    }


}
