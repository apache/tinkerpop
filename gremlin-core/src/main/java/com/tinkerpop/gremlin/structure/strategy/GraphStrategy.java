package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.util.function.TriFunction;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Defines a collection of functions that plug-in to Gremlin Structure API methods to enhance or alter the functionality of
 * the implementation. The methods defined in {@link GraphStrategy} follow a common pattern where each method
 * represents some injection point for new logic in the Gremlin Structure API.  A method always accepts a
 * {@link StrategyContext} which contains the context of the call being made and will have
 * a different {@link StrategyContext#getCurrent()} object depending on that context (e.g the
 * {@link com.tinkerpop.gremlin.structure.Vertex#addEdge(String, com.tinkerpop.gremlin.structure.Vertex, Object...)} method will send the instance of
 * the {@link com.tinkerpop.gremlin.structure.Vertex} that was the object of that method call).
 * <p/>
 * A method will always return a {@link java.util.function.UnaryOperator} where the argument and return value to it is a function with
 * the same signature as the calling method where the strategy logic is being injected.  The argument passed in to
 * this function will be a reference to the original calling function (from an implementation perspective, it might
 * be best to think of this "calling function" as the original Gremlin Structure API method that performs the ultimate
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
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphStrategy {

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph.Iterators#vertexIterator}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Graph.Iterators#vertexIterator} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Function<Object[], Iterator<Vertex>>> getGraphIteratorsVertexIteratorStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph.Iterators#edgeIterator}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Graph.Iterators#edgeIterator} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Function<Object[], Iterator<Edge>>> getGraphIteratorsEdgeIteratorStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph.Variables#keys()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Graph.Variables#keys()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Set<String>>> getVariableKeysStrategy(final StrategyContext<StrategyVariables> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph.Variables#asMap()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Graph.Variables#asMap()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Map<String, Object>>> getVariableAsMapStrategy(final StrategyContext<StrategyVariables> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph.Variables#get(String)}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with {@link com.tinkerpop.gremlin.structure.Graph.Variables#get(String)} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default <R> UnaryOperator<Function<String, Optional<R>>> getVariableGetStrategy(final StrategyContext<StrategyVariables> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.BiConsumer} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph.Variables#set(String, Object)}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.BiConsumer} with {@link com.tinkerpop.gremlin.structure.Graph.Variables#set(String, Object)} signature
     * and returns an enhanced strategy {@link java.util.function.BiConsumer} with the same signature
     */
    public default UnaryOperator<BiConsumer<String, Object>> getVariableSetStrategy(final StrategyContext<StrategyVariables> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Consumer} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph.Variables#remove(String)}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Consumer} with {@link com.tinkerpop.gremlin.structure.Graph.Variables#remove(String)} signature
     * and returns an enhanced strategy {@link java.util.function.BiConsumer} with the same signature
     */
    public default UnaryOperator<Consumer<String>> getVariableRemoveStrategy(final StrategyContext<StrategyVariables> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph#addVertex(Object...)}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with {@link com.tinkerpop.gremlin.structure.Graph#addVertex(Object...)} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link com.tinkerpop.gremlin.util.function.TriFunction} that enhances the features of
     * {@link com.tinkerpop.gremlin.structure.Vertex#addEdge(String, com.tinkerpop.gremlin.structure.Vertex, Object...)}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link com.tinkerpop.gremlin.util.function.TriFunction} with
     * {@link com.tinkerpop.gremlin.structure.Vertex#addEdge(String, com.tinkerpop.gremlin.structure.Vertex, Object...)} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge#remove()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Edge#remove()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Void>> getRemoveEdgeStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex#remove()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Vertex#remove()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Void>> getRemoveVertexStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Property#remove()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Property#remove()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Supplier<Void>> getRemovePropertyStrategy(final StrategyContext<StrategyProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.VertexProperty#remove()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.VertexProperty#remove()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Supplier<Void>> getRemoveVertexPropertyStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex#property(String)}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Vertex#property(String)} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default <V> UnaryOperator<Function<String, VertexProperty<V>>> getVertexGetPropertyStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex#keys()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Vertex#keys()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Set<String>>> getVertexKeysStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex#label()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Vertex#label()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<String>> getVertexLabelStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.BiFunction} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex#property(String, Object)}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.BiFunction} with
     * {@link com.tinkerpop.gremlin.structure.Vertex#property(String, Object)} signature
     * and returns an enhanced strategy {@link java.util.function.BiFunction} with the same signature
     */
    public default <V> UnaryOperator<BiFunction<String, V, VertexProperty<V>>> getVertexPropertyStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Element.Iterators#propertyIterator}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Element.Iterators#propertyIterator} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Function<String[], Iterator<VertexProperty<V>>>> getVertexIteratorsPropertyIteratorStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link TriFunction} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex.Iterators#vertexIterator}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link BiFunction} with
     * {@link com.tinkerpop.gremlin.structure.Vertex.Iterators#vertexIterator} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<BiFunction<Direction, String[], Iterator<Vertex>>> getVertexIteratorsVertexIteratorStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link TriFunction} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex.Iterators#edgeIterator}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link BiFunction} with
     * {@link com.tinkerpop.gremlin.structure.Vertex.Iterators#edgeIterator} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<BiFunction<Direction, String[], Iterator<Edge>>> getVertexIteratorsEdgeIteratorStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex.Iterators#valueIterator}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Vertex.Iterators#valueIterator} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Function<String[], Iterator<V>>> getVertexIteratorsValueIteratorStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex#id()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Vertex#id()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Object>> getVertexIdStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex#graph()}.
     * Note that in this case, the {@link Graph} is {@link StrategyGraph} and this would be the expected
     * type to pass back out.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Vertex#graph()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Graph>> getVertexGraphStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Element#value(String)}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Element#value(String)} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default <V> UnaryOperator<Function<String, V>> getVertexValueStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge#property(String)}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Edge#property(String)} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default <V> UnaryOperator<Function<String, Property<V>>> getEdgeGetPropertyStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge.Iterators#vertexIterator}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Edge.Iterators#vertexIterator} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Function<Direction, Iterator<Vertex>>> getEdgeIteratorsVertexIteratorStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge.Iterators#valueIterator}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Edge.Iterators#valueIterator} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Function<String[], Iterator<V>>> getEdgeIteratorsValueIteratorStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge.Iterators#propertyIterator}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Edge.Iterators#propertyIterator} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Function<String[], Iterator<Property<V>>>> getEdgeIteratorsPropertyIteratorStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge#keys()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Edge#keys()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Set<String>>> getEdgeKeysStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.BiFunction} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge#property(String, Object)}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.BiFunction} with
     * {@link com.tinkerpop.gremlin.structure.Edge#property(String, Object)} signature
     * and returns an enhanced strategy {@link java.util.function.BiFunction} with the same signature
     */
    public default <V> UnaryOperator<BiFunction<String, V, Property<V>>> getEdgePropertyStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge#value(String)}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Element#value(String)} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default <V> UnaryOperator<Function<String, V>> getEdgeValueStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge#id()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Edge#id()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Object>> getEdgeIdStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge#graph()}.
     * Note that in this case, the {@link Graph} is {@link StrategyGraph} and this would be the expected
     * type to pass back out.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Edge#graph()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Graph>> getEdgeGraphStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge#label()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Edge#label()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<String>> getEdgeLabelStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.VertexProperty#id()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.VertexProperty#id()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Supplier<Object>> getVertexPropertyIdStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.VertexProperty#value()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Supplier} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.VertexProperty#value()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Supplier<V>> getVertexPropertyValueStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Property#key()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Supplier} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Property#key()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Supplier<String>> getVertexPropertyKeyStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.VertexProperty.Iterators#propertyIterator}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.VertexProperty.Iterators#propertyIterator} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V, U> UnaryOperator<Function<String[], Iterator<Property<V>>>> getVertexPropertyIteratorsPropertyIteratorStrategy(final StrategyContext<StrategyVertexProperty<U>> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.VertexProperty.Iterators#valueIterator}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.VertexProperty.Iterators#valueIterator} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V, U> UnaryOperator<Function<String[], Iterator<V>>> getVertexPropertyIteratorsValueIteratorStrategy(final StrategyContext<StrategyVertexProperty<U>> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.BiFunction} that enhances the features of {@link com.tinkerpop.gremlin.structure.VertexProperty#property(String, Object)}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.BiFunction} with
     * {@link com.tinkerpop.gremlin.structure.VertexProperty#property(String, Object)} signature
     * and returns an enhanced strategy {@link java.util.function.BiFunction} with the same signature
     */
    public default <V, U> UnaryOperator<BiFunction<String, V, Property<V>>> getVertexPropertyPropertyStrategy(final StrategyContext<StrategyVertexProperty<U>> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.VertexProperty#graph()}.
     * Note that in this case, the {@link Graph} is {@link StrategyGraph} and this would be the expected
     * type to pass back out.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.VertexProperty#graph()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Supplier<Graph>> getVertexPropertyGraphStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.VertexProperty#label()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.VertexProperty#label()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Supplier<String>> getVertexPropertyLabelStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.VertexProperty#keys()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.VertexProperty#keys()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Supplier<Set<String>>> getVertexPropertyKeysStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.VertexProperty#element}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.VertexProperty#element} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Supplier<Vertex>> getVertexPropertyGetElementStrategy(final StrategyContext<StrategyVertexProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph#close()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Supplier} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Graph#close()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Void>> getGraphCloseStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph#V}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Graph#V} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default UnaryOperator<Function<Object[], GraphTraversal<Vertex, Vertex>>> getGraphVStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph#E}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Graph#E} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default UnaryOperator<Function<Object[], GraphTraversal<Edge, Edge>>> getGraphEStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Property#value()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Supplier} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Property#value()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Supplier<V>> getPropertyValueStrategy(final StrategyContext<StrategyProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Property#key()}.
     *
     * @param ctx               the context within which this strategy function is called
     * @param composingStrategy the strategy that composed this strategy function
     * @return a {@link java.util.function.Supplier} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Property#key()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Supplier<String>> getPropertyKeyStrategy(final StrategyContext<StrategyProperty<V>> ctx, final GraphStrategy composingStrategy) {
        return UnaryOperator.identity();
    }

}
