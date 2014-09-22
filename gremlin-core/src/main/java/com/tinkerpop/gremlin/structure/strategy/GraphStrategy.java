package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.util.function.STriFunction;

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
 * {@link Strategy.Context} which contains the context of the call being made and will have
 * a different {@link Strategy.Context#getCurrent()} object depending on that context (e.g the
 * {@link com.tinkerpop.gremlin.structure.Vertex#addEdge(String, com.tinkerpop.gremlin.structure.Vertex, Object...)} method will send the instance of
 * the {@link com.tinkerpop.gremlin.structure.Vertex} that was the object of that method call).
 * <p>
 * A method will always return a {@link java.util.function.UnaryOperator} where the argument and return value to it is a function with
 * the same signature as the calling method where the strategy logic is being injected.  The argument passed in to
 * this function will be a reference to the original calling function (from an implementation perspective, it might
 * be best to think of this "calling function" as the original Gremlin Structure API method that performs the ultimate
 * operation against the graph).  In constructing the outgoing function to the {@link java.util.function.UnaryOperator}, it should
 * of course match the signature of the original calling function and depending on the functionality,
 * call the original function to trigger a call to the underlying implementation.
 * <p>
 * The most simplistic implementation of a strategy method is to simply return a
 * {@link java.util.function.UnaryOperator#identity()} which happens to be the default implementation for all the
 * methods.  By returning the {@code identity} function, the incoming original function is simply returned back
 * unchanged with no additional enhancements to the execution of it.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphStrategy {
    /**
     * For all methods that return a {@link GraphTraversal} add a {@link com.tinkerpop.gremlin.process.TraversalStrategy}.
     * The {@link StrategyWrapped} classes will automatically add steps that will ensure that all items that come
     * out of the traversal are wrapped appropriately.
     */
    public default GraphTraversal applyStrategyToTraversal(final GraphTraversal traversal) {
        return traversal;
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph.Variables#keys()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Graph.Variables#keys()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Set<String>>> getVariableKeysStrategy(final Strategy.Context<StrategyWrappedVariables> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph.Variables#asMap()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Graph.Variables#asMap()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Map<String, Object>>> getVariableAsMapStrategy(final Strategy.Context<StrategyWrappedVariables> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph.Variables#get(String)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with {@link com.tinkerpop.gremlin.structure.Graph.Variables#get(String)} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default <R> UnaryOperator<Function<String, Optional<R>>> getVariableGetStrategy(final Strategy.Context<StrategyWrappedVariables> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.BiConsumer} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph.Variables#set(String, Object)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.BiConsumer} with {@link com.tinkerpop.gremlin.structure.Graph.Variables#set(String, Object)} signature
     * and returns an enhanced strategy {@link java.util.function.BiConsumer} with the same signature
     */
    public default UnaryOperator<BiConsumer<String, Object>> getVariableSetStrategy(final Strategy.Context<StrategyWrappedVariables> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Consumer} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph.Variables#remove(String)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Consumer} with {@link com.tinkerpop.gremlin.structure.Graph.Variables#remove(String)} signature
     * and returns an enhanced strategy {@link java.util.function.BiConsumer} with the same signature
     */
    public default UnaryOperator<Consumer<String>> getVariableRemoveStrategy(final Strategy.Context<StrategyWrappedVariables> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph#addVertex(Object...)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with {@link com.tinkerpop.gremlin.structure.Graph#addVertex(Object...)} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link com.tinkerpop.gremlin.util.function.STriFunction} that enhances the features of
     * {@link com.tinkerpop.gremlin.structure.Vertex#addEdge(String, com.tinkerpop.gremlin.structure.Vertex, Object...)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Vertex#addEdge(String, com.tinkerpop.gremlin.structure.Vertex, Object...)} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default UnaryOperator<STriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge#remove()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Edge#remove()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Void>> getRemoveEdgeStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex#remove()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Vertex#remove()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Void>> getRemoveVertexStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Property#remove()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Property#remove()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Supplier<Void>> getRemovePropertyStrategy(final Strategy.Context<StrategyWrappedProperty<V>> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.VertexProperty#remove()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.VertexProperty#remove()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Supplier<Void>> getRemoveVertexPropertyStrategy(final Strategy.Context<StrategyWrappedVertexProperty<V>> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex#property(String)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Vertex#property(String)} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default <V> UnaryOperator<Function<String, VertexProperty<V>>> getVertexGetPropertyStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge#property(String)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Edge#property(String)} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default <V> UnaryOperator<Function<String, Property<V>>> getEdgeGetPropertyStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.BiFunction} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex#property(String, Object)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.BiFunction} with
     * {@link com.tinkerpop.gremlin.structure.Vertex#property(String, Object)} signature
     * and returns an enhanced strategy {@link java.util.function.BiFunction} with the same signature
     */
    public default <V> UnaryOperator<BiFunction<String, V, VertexProperty<V>>> getVertexPropertyStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.BiFunction} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge#property(String, Object)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.BiFunction} with
     * {@link com.tinkerpop.gremlin.structure.Edge#property(String, Object)} signature
     * and returns an enhanced strategy {@link java.util.function.BiFunction} with the same signature
     */
    public default <V> UnaryOperator<BiFunction<String, V, Property<V>>> getEdgePropertyStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Element.Iterators#properties}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Element.Iterators#properties} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Function<String[],Iterator<VertexProperty<V>>>> getVertexIteratorsPropertiesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge.Iterators#properties}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Edge.Iterators#properties} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Function<String[],Iterator<Property<V>>>> getEdgeIteratorsPropertiesStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex.Iterators#hiddens}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Vertex.Iterators#hiddens} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Function<String[],Iterator<VertexProperty<V>>>> getVertexIteratorsHiddensStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge.Iterators#hiddens}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Edge.Iterators#hiddens} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Function<String[],Iterator<? extends Property<V>>>> getEdgeIteratorsHiddensStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex.Iterators#vertices}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Vertex.Iterators#vertices} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<STriFunction<Direction, Integer, String[], Iterator<Vertex>>> getVertexIteratorsVerticesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex.Iterators#vertices}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Vertex.Iterators#vertices} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<STriFunction<Direction, Integer, String[], Iterator<Edge>>> getVertexIteratorsEdgesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Element#value(String)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Element#value(String)} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default <V> UnaryOperator<Function<String, V>> getVertexValueStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge#value(String)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Element#value(String)} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default <V> UnaryOperator<Function<String, V>> getEdgeValueStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex#id()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Vertex#id()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Object>> getVertexIdStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge#id()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Edge#id()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Object>> getEdgeIdStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex#label()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Vertex#label()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<String>> getVertexLabelStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge#label()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Edge#label()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<String>> getEdgeLabelStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex#keys()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Vertex#keys()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Set<String>>> getVertexKeysStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge#keys()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Edge#keys()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Set<String>>> getEdgeKeysStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex#hiddenKeys()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Vertex#hiddenKeys()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Set<String>>> getVertexHiddenKeysStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge#hiddenKeys()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Edge#hiddenKeys()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Set<String>>> getEdgeHiddenKeysStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex.Iterators#values}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Vertex.Iterators#values} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Function<String[],Iterator<V>>> getVertexIteratorsValuesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge.Iterators#values}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Edge.Iterators#values} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Function<String[],Iterator<V>>> getEdgeIteratorsValuesStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Vertex.Iterators#hiddenValues}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Vertex.Iterators#hiddenValues} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Function<String[],Iterator<V>>> getVertexIteratorsHiddenValuesStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Edge.Iterators#hiddenValues}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Edge.Iterators#hiddenValues} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default <V> UnaryOperator<Function<String[],Iterator<V>>> getEdgeIteratorsHiddenValuesStrategy(final Strategy.Context<StrategyWrappedEdge> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph#v(Object)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Graph#v(Object)} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default UnaryOperator<Function<Object, Vertex>> getGraphvStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Function} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph#e(Object)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Function} that accepts a {@link java.util.function.Function} with
     * {@link com.tinkerpop.gremlin.structure.Graph#e(Object)} signature
     * and returns an enhanced strategy {@link java.util.function.Function} with the same signature
     */
    public default UnaryOperator<Function<Object, Edge>> getGrapheStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link java.util.function.Supplier} that enhances the features of {@link com.tinkerpop.gremlin.structure.Graph#close()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link java.util.function.Supplier} that accepts a {@link java.util.function.Supplier} with
     * {@link com.tinkerpop.gremlin.structure.Graph#close()} signature
     * and returns an enhanced strategy {@link java.util.function.Supplier} with the same signature
     */
    public default UnaryOperator<Supplier<Void>> getGraphCloseStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return UnaryOperator.identity();
    }

    public static class DoNothingGraphStrategy implements GraphStrategy {
        public static final DoNothingGraphStrategy INSTANCE = new DoNothingGraphStrategy();

        private DoNothingGraphStrategy() {
        }

        @Override
        public String toString() {
            return "passthru";
        }
    }
}
