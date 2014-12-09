package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.strategy.GraphStrategy;
import com.tinkerpop.gremlin.structure.strategy.Strategy;
import com.tinkerpop.gremlin.structure.strategy.StrategyWrappedGraph;
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
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ComputerDataStrategy implements GraphStrategy {

    private final Set<String> elementComputeKeys;

    public ComputerDataStrategy(final Set<String> elementComputeKeys) {
        this.elementComputeKeys = elementComputeKeys;
    }

    @Override
    public <V> UnaryOperator<Function<String[], Iterator<VertexProperty<V>>>> getVertexIteratorsPropertiesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return (f) -> (keys) -> keys.length == 0 ? IteratorUtils.filter(f.apply(keys), property -> !this.elementComputeKeys.contains(property.key())) : f.apply(keys);
    }

    /*@Override
    public <V> UnaryOperator<Function<String[], Iterator<V>>> getVertexIteratorsValuesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {

    }*/

    @Override
    public UnaryOperator<Supplier<Set<String>>> getVertexKeysStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return (f) -> () -> IteratorUtils.fill(IteratorUtils.filter(f.get().iterator(), key -> !this.elementComputeKeys.contains(key)), new HashSet<>());
    }


    public static StrategyWrappedGraph wrapGraph(final Graph graph, final VertexProgram<?> vertexProgram) {
        final StrategyWrappedGraph sg = new StrategyWrappedGraph(graph);
        sg.getStrategy().setGraphStrategy(new ComputerDataStrategy(vertexProgram.getElementComputeKeys()));
        return sg;
    }

    public static StrategyWrappedVertex wrapVertex(final Vertex vertex, final VertexProgram<?> vertexProgram) {
        final StrategyWrappedGraph sg = new StrategyWrappedGraph(vertex.graph());
        sg.getStrategy().setGraphStrategy(new ComputerDataStrategy(vertexProgram.getElementComputeKeys()));
        return new StrategyWrappedVertex(vertex, sg);
    }

}
