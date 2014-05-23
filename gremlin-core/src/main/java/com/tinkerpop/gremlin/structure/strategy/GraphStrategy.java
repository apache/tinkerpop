package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.TriFunction;

import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Defines a collection of functions that plug-in to Blueprints API methods to enhance or alter the functionality of
 * the implementation. The methods defined in {@link GraphStrategy} follow a common pattern where each method
 * represents some injection point for new logic in the Blueprints API.  A method always accepts a
 * {@link Strategy.Context} which contains the context of the call being made and will have
 * a different {@link Strategy.Context#getCurrent()} object depending on that context (e.g the
 * {@link com.tinkerpop.gremlin.structure.Vertex#addEdge(String, com.tinkerpop.gremlin.structure.Vertex, Object...)} method will send the instance of
 * the {@link com.tinkerpop.gremlin.structure.Vertex} that was the object of that method call).
 * <p/>
 * A method will always return a {@link java.util.function.UnaryOperator} where the argument and return value to it is a function with
 * the same signature as the calling method where the strategy logic is being injected.  The argument passed in to
 * this function will be a reference to the original calling function (from an implementation perspective, it might
 * be best to think of this "calling function" as the original Blueprints API method that performs the ultimate
 * operation against the graph).  In constructing the outgoing function to the {@link java.util.function.UnaryOperator}, it should
 * of course match the signature of the original calling function and depending on the functionality,
 * call the original function to trigger a call to the underlying implementation.
 * <p/>
 * The most simplistic implementation of a strategy method is to simply return a
 * {@link java.util.function.UnaryOperator#identity()} which happens to be the default implementation for all the
 * methods.  By returning the {@code identity} function, the incoming original function is simply returned back
 * unchanged with no additional enhancements to the execution of it.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphStrategy {
    // todo: review new element methods.

    /**
     * For all methods that return a {@link GraphTraversal} add a {@link com.tinkerpop.gremlin.process.TraversalStrategy}.
     * The {@link StrategyWrapped} classes will automatically add steps that will ensure that all items that come
     * out of the traversal are wrapped appropriately.
     */
    public default GraphTraversal applyStrategyToTraversal(final GraphTraversal traversal) {
        return traversal;
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph#addVertex(Object...)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with {@link com.tinkerpop.gremlin.structure.Graph#addVertex(Object...)} signature
     *         and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default UnaryOperator<Function<Object[],Vertex>> getAddVertexStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link com.tinkerpop.gremlin.util.function.TriFunction} that enhances the features of
     * {@link com.tinkerpop.gremlin.structure.Vertex#addEdge(String, com.tinkerpop.gremlin.structure.Vertex, Object...)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     *         {@link com.tinkerpop.gremlin.structure.Vertex#addEdge(String, com.tinkerpop.gremlin.structure.Vertex, Object...)} signature
     *         and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Element#remove()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     *         {@link com.tinkerpop.gremlin.structure.Element#remove()} signature
     *         and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Void>> getRemoveElementStrategy(final Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Property#remove()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     *         {@link com.tinkerpop.gremlin.structure.Property#remove()} signature
     *         and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Supplier<Void>> getRemovePropertyStrategy(final Strategy.Context<StrategyWrappedProperty<V>> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Element#property(String)}.
     * If a strategy must implement different scenarios for a {@link com.tinkerpop.gremlin.structure.Vertex} versus an {@link com.tinkerpop.gremlin.structure.Edge} the implementation
     * should check for the type of {@link com.tinkerpop.gremlin.structure.Element} on the {@link Strategy.Context}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     *         {@link com.tinkerpop.gremlin.structure.Element#property(String)} signature
     *         and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default <V> UnaryOperator<Function<String, Property<V>>> getElementGetProperty(final Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.BiFunction} that enhances the features of {@link com.tinkerpop.gremlin.structure.Element#property(String, Object)}.
     * If a strategy must implement different scenarios for a {@link com.tinkerpop.gremlin.structure.Vertex} versus an {@link com.tinkerpop.gremlin.structure.Edge} the implementation
     * should check for the type of {@link com.tinkerpop.gremlin.structure.Element} on the {@link Strategy.Context}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.BiFunction} with
     *         {@link com.tinkerpop.gremlin.structure.Element#property(String,Object)} signature
     *         and returns an enhanced strategy {@link java.util.function.BiFunction} with the same signature
     */
    public default <V> UnaryOperator<BiFunction<String, V, Property<V>>> getElementProperty(final Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return UnaryOperator.identity();
    }

	/**
	 * Construct a {@link java.util.function.Consumer} that enhances the features of {@link com.tinkerpop.gremlin.structure.Element#properties(Object...)}.
	 * If a strategy must implement different scenarios for a {@link com.tinkerpop.gremlin.structure.Vertex} versus an {@link com.tinkerpop.gremlin.structure.Edge} the implementation
	 * should check for the type of {@link com.tinkerpop.gremlin.structure.Element} on the {@link Strategy.Context}.
	 *
	 * @param ctx the context within which this strategy function is called
	 * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Consumer} with
	 *         {@link com.tinkerpop.gremlin.structure.Element#properties(Object...)} signature
	 *         and returns an enhanced strategy {@link java.util.function.Consumer} with the same signature
	 */
	public default UnaryOperator<Consumer<Object[]>> getElementProperties(final Strategy.Context<? extends StrategyWrappedElement> ctx) {
		return UnaryOperator.identity();
	}

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Element#id()}.
     * If a strategy must implement different scenarios for a {@link com.tinkerpop.gremlin.structure.Vertex} versus an {@link com.tinkerpop.gremlin.structure.Edge} the implementation
     * should check for the type of {@link com.tinkerpop.gremlin.structure.Element} on the {@link Strategy.Context}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     *         {@link com.tinkerpop.gremlin.structure.Element#id()} signature
     *         and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Object>> getElementId(final Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Element#label()}.
     * If a strategy must implement different scenarios for a {@link com.tinkerpop.gremlin.structure.Vertex} versus an {@link com.tinkerpop.gremlin.structure.Edge} the implementation
     * should check for the type of {@link com.tinkerpop.gremlin.structure.Element} on the {@link Strategy.Context}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     *         {@link com.tinkerpop.gremlin.structure.Element#label()} signature
     *         and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<String>> getElementLabel(final Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Element#keys()}.
     * If a strategy must implement different scenarios for a {@link com.tinkerpop.gremlin.structure.Vertex} versus an {@link com.tinkerpop.gremlin.structure.Edge} the implementation
     * should check for the type of {@link com.tinkerpop.gremlin.structure.Element} on the {@link Strategy.Context}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     *         {@link com.tinkerpop.gremlin.structure.Element#keys()} signature
     *         and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Set<String>>> getElementKeys(final Strategy.Context<? extends StrategyWrappedElement> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph#v(Object)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     *         {@link com.tinkerpop.gremlin.structure.Graph#v(Object)} signature
     *         and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default UnaryOperator<Function<Object, Vertex>> getGraphvStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph#e(Object)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     *         {@link com.tinkerpop.gremlin.structure.Graph#e(Object)} signature
     *         and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default UnaryOperator<Function<Object, Edge>> getGrapheStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return UnaryOperator.identity();
    }

	public static class DoNothingGraphStrategy implements GraphStrategy {
		public static final DoNothingGraphStrategy INSTANCE = new DoNothingGraphStrategy();

		private DoNothingGraphStrategy() {}

		@Override
		public String toString() {
			return "passthru";
		}
	}
}
