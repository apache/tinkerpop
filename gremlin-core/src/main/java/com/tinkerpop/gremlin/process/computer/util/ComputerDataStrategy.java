package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.strategy.GraphStrategy;
import com.tinkerpop.gremlin.structure.strategy.Strategy;
import com.tinkerpop.gremlin.structure.strategy.StrategyWrappedVertex;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ComputerDataStrategy implements GraphStrategy {

    private final String prefix;

    public ComputerDataStrategy(final String prefix) {
        this.prefix = prefix;
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<VertexProperty<V>>>> getVertexIteratorsPropertiesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return (f) -> (keys) -> {
            if (keys.length == 0)
                return IteratorUtils.filter(f.apply(keys), property -> !property.key().startsWith(prefix));

            return f.apply(keys);
        };
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getVertexKeysStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return (f) -> () -> {
            // todo: this is dumb = fixy fix
            final Iterator<String> keys = f.get().iterator();
            final Iterator<String> filteredKeys = IteratorUtils.filter(keys, k -> !k.startsWith(prefix));
            return new HashSet<>(IteratorUtils.convertToList(filteredKeys));
        };
    }
}
