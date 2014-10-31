package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GlobalTraversalStrategyCache {

    private static final Map<Class<? extends Traversal>, Traversal.Strategies> CACHE = new HashMap<>();

    public static final <T extends Traversal.Strategies> T getOrCreate(final Traversal traversal, Supplier<Traversal.Strategies> strategiesSupplier) {
        final Traversal.Strategies strategies = CACHE.get(traversal.getClass());
        if (null == strategies) {
            final Traversal.Strategies newStrategies = strategiesSupplier.get();
            CACHE.put(traversal.getClass(), newStrategies);
            return (T) newStrategies;
        } else {
            return (T) strategies;
        }
    }
}
