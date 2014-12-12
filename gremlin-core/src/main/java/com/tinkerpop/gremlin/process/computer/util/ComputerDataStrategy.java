package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.strategy.GraphStrategy;
import com.tinkerpop.gremlin.structure.strategy.StrategyContext;
import com.tinkerpop.gremlin.structure.strategy.StrategyGraph;
import com.tinkerpop.gremlin.structure.strategy.StrategyVertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ComputerDataStrategy implements GraphStrategy {

    private final Set<String> elementComputeKeys;

    public ComputerDataStrategy(final Set<String> elementComputeKeys) {
        this.elementComputeKeys = elementComputeKeys;
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<VertexProperty<V>>>> getVertexIteratorsPropertyIteratorStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy strategyComposer) {
        return (f) -> (keys) -> keys.length == 0 ? IteratorUtils.filter(f.apply(keys), property -> !this.elementComputeKeys.contains(property.key())) : f.apply(keys);
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<V>>> getVertexIteratorsValueIteratorStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy strategyComposer) {
        return (f) -> (keys) -> IteratorUtils.map(ctx.getCurrent().iterators().<V>propertyIterator(keys), vertexProperty -> vertexProperty.value());
    }

    @Override
    public UnaryOperator<Supplier<Set<String>>> getVertexKeysStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy strategyComposer) {
        return (f) -> () -> IteratorUtils.fill(IteratorUtils.filter(f.get().iterator(), key -> !this.elementComputeKeys.contains(key)), new HashSet<>());
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyString(this);
    }

    public static StrategyGraph wrapGraph(final Graph graph, final VertexProgram<?> vertexProgram) {
        return new StrategyGraph(graph, new ComputerDataStrategy(vertexProgram.getElementComputeKeys()));
    }

    public static StrategyVertex wrapVertex(final Vertex vertex, final VertexProgram<?> vertexProgram) {
        final StrategyGraph sg = new StrategyGraph(vertex.graph(), new ComputerDataStrategy(vertexProgram.getElementComputeKeys()));
        return new StrategyVertex(vertex, sg);
    }

}
