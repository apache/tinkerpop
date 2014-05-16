package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Optimizer;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.map.EdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.map.GraphStep;
import com.tinkerpop.gremlin.process.graph.map.MapStep;
import com.tinkerpop.gremlin.process.graph.map.VertexStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.util.function.TriFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * This {@link GraphStrategy} prevents the graph from being modified and will throw a
 * {@link UnsupportedOperationException} if an attempt is made to do so.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ReadOnlyGraphStrategy implements GraphStrategy {
    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return readOnlyFunction();
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return readOnlyTriFunction();
    }

    @Override
    public <V> UnaryOperator<BiFunction<String, V, Property<V>>> getElementProperty(Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return readOnlyBiFunction();
    }

	@Override
	public UnaryOperator<Consumer<Object[]>> getElementProperties(Strategy.Context<? extends StrategyWrappedElement> ctx) {
		return (f) -> (t) -> { throw Exceptions.graphUsesReadOnlyStrategy(); };
	}

	@Override
    public UnaryOperator<Supplier<Void>> getRemoveElementStrategy(final Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return readOnlySupplier();
    }

    @Override
    public <V> UnaryOperator<Supplier<Void>> getRemovePropertyStrategy(final Strategy.Context<StrategyWrappedProperty<V>> ctx) {
        return readOnlySupplier();
    }

	@Override
	public UnaryOperator<Supplier<GraphTraversal<Vertex, Vertex>>> getVStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
		return (f) -> () -> {
			final GraphTraversal traversal = f.get();
			traversal.optimizers().register(new ReadOnlyGraphTraversalOptimizer(ctx.getCurrent()));
			return traversal;
		};
	}

	@Override
	public UnaryOperator<Supplier<GraphTraversal<Edge, Edge>>> getEStrategy(Strategy.Context<StrategyWrappedGraph> ctx) {
		return (f) -> () -> {
			final GraphTraversal traversal = f.get();
			traversal.optimizers().register(new ReadOnlyGraphTraversalOptimizer(ctx.getCurrent()));
			return traversal;
		};
	}

	@Override
	public String toString() {
		return ReadOnlyGraphStrategy.class.getSimpleName().toLowerCase();
	}

    public static <T> UnaryOperator<Supplier<T>> readOnlySupplier() {
        return (f) -> () -> { throw Exceptions.graphUsesReadOnlyStrategy(); };
    }

    public static <T, U> UnaryOperator<Function<T, U>> readOnlyFunction() {
        return (f) -> (t) -> { throw Exceptions.graphUsesReadOnlyStrategy(); };
    }

    public static <T, U> UnaryOperator<BiFunction<T, U, Property<U>>> readOnlyBiFunction() {
        return (f) -> (t,u) -> { throw Exceptions.graphUsesReadOnlyStrategy(); };
    }

    public static <T, U, V, W> UnaryOperator<TriFunction<T, U, V, W>> readOnlyTriFunction() {
        return (f) -> (t, u, v) -> { throw Exceptions.graphUsesReadOnlyStrategy(); };
    }

    public static class Exceptions {
        public static UnsupportedOperationException graphUsesReadOnlyStrategy() {
            return new UnsupportedOperationException(String.format("Graph uses %s and is therefore unmodifiable", ReadOnlyGraphStrategy.class));
        }
    }

	/**
	 * Analyzes the traversal and injects the readonly logic after every access to a vertex or edge.  The readonly
	 * logic consists of a {@link MapStep} that wraps the @{link Element} back up in a {@link StrategyWrapped}
	 * implementation.
	 */
	public static class ReadOnlyGraphTraversalOptimizer implements Optimizer.FinalOptimizer {
		private final StrategyWrappedGraph graph;

		public ReadOnlyGraphTraversalOptimizer(final StrategyWrappedGraph graph) {
			this.graph = graph;
		}

		public void optimize(final Traversal traversal) {
			// inject a HasStep after each GraphStep, VertexStep or EdgeVertexStep
			final List<Class> stepsToLookFor = Arrays.<Class>asList(GraphStep.class, VertexStep.class, EdgeVertexStep.class);
			final List<Integer> positions = new ArrayList<>();
			final List<?> traversalSteps = traversal.getSteps();
			for (int ix = 0; ix < traversalSteps.size(); ix++) {
				final int pos = ix;
				if (stepsToLookFor.stream().anyMatch(c -> c.isAssignableFrom(traversalSteps.get(pos).getClass()))) positions.add(ix);
			}

			Collections.reverse(positions);
			for (int pos : positions) {
				final MapStep<Object, Object> transformToStrategy = new MapStep<>(traversal);
				transformToStrategy.setFunction((Holder<Object> t) -> {
					// todo: need to make sure we're not re-wrapping in strategy over and over again.
					final Object o = t.get();
					if (o instanceof Vertex)
						return new StrategyWrappedVertex((Vertex) o, graph);
					else if (o instanceof Edge)
						return new StrategyWrappedEdge((Edge) o, graph);
					else if (o instanceof Property)
						return new StrategyWrappedProperty((Property) o, graph);
					else
						return o;
				});

				TraversalHelper.insertStep(transformToStrategy, pos + 1, traversal);
			}
		}
	}
}
