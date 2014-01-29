package com.tinkerpop.blueprints.strategy;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Strategy;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.GraphQuery;
import com.tinkerpop.blueprints.util.function.TriFunction;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Defines a collection of functions that plug-in to Blueprints API methods to enhance or alter the functionality of
 * the implementation. The methods defined in {@link GraphStrategy} follow a common pattern where each method
 * represents some injection point for new logic in the Blueprints API.  A method always accepts a
 * {@link com.tinkerpop.blueprints.Strategy.Context} which contains the context of the call being made and will have
 * a different {@link com.tinkerpop.blueprints.Strategy.Context#getCurrent()} object depending on that context (e.g the
 * {@link Vertex#addEdge(String, com.tinkerpop.blueprints.Vertex, Object...)} method will send the instance of
 * the {@link Vertex} that was the object of that method call).
 * <p/>
 * A method will always return a {@link UnaryOperator} where the argument and return value to it is a function with
 * the same signature as the calling method where the strategy logic is being injected.  The argument passed in to
 * this function will be a reference to the original calling function (from an implementation perspective, it might
 * be best to think of this "calling function" as the original Blueprints API method that performs the ultimate
 * operation against the graph).  In constructing the outgoing function to the {@link UnaryOperator}, it should
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

    /**
     * Construct a {@link Function} that enhances the features of {@link Graph#addVertex(Object...)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link Function} that accepts a {@link Function} with {@link Graph#addVertex(Object...)} signature
     *         and returns an enhanced strategy {@link Function} with the same signature
     */
    public default UnaryOperator<Function<Object[],Vertex>> getAddVertexStrategy(final Strategy.Context<Graph> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link TriFunction} that enhances the features of
     * {@link Vertex#addEdge(String, com.tinkerpop.blueprints.Vertex, Object...)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link Function} that accepts a {@link Function} with
     *         {@link Vertex#addEdge(String, com.tinkerpop.blueprints.Vertex, Object...)} signature
     *         and returns an enhanced strategy {@link Function} with the same signature
     */
    public default UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.Context<Vertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link Supplier} that enhances the features of {@link com.tinkerpop.blueprints.Vertex#remove()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link Function} that accepts a {@link Function} with
     *         {@link com.tinkerpop.blueprints.Vertex#remove()} signature
     *         and returns an enhanced strategy {@link Function} with the same signature
     */
    public default UnaryOperator<Supplier<Void>> getRemoveVertexStrategy(final Strategy.Context<Vertex> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link Function} that enhances the features of {@link com.tinkerpop.blueprints.Element#getProperty(String)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link Function} that accepts a {@link Function} with
     *         {@link com.tinkerpop.blueprints.Element#getProperty(String)} ()} signature
     *         and returns an enhanced strategy {@link Function} with the same signature
     */
    public default <V> UnaryOperator<Function<String, Property<V>>> getElementGetProperty(final Strategy.Context<Element> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link Supplier} that enhances the features of
     * {@link com.tinkerpop.blueprints.query.GraphQuery#vertices()}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link Function} that accepts a {@link Function} with
     *         {@link com.tinkerpop.blueprints.query.GraphQuery#vertices()} signature
     *         and returns an enhanced strategy {@link Function} with the same signature
     */
    public default UnaryOperator<Supplier<Iterable<Vertex>>> getGraphQueryVerticesStrategy(final Strategy.Context<GraphQuery> ctx) {
        return UnaryOperator.identity();
    }

    /**
     * Construct a {@link Function} that enhances the features of
     * {@link com.tinkerpop.blueprints.query.GraphQuery#ids(Object...)}.
     *
     * @param ctx the context within which this strategy function is called
     * @return a {@link Function} that accepts a {@link Function} with
     *         {@link com.tinkerpop.blueprints.query.GraphQuery#ids(Object...)} signature
     *         and returns an enhanced strategy {@link Function} with the same signature
     */
    public default UnaryOperator<Function<Object[], GraphQuery>> getGraphQueryIdsStrategy(final Strategy.Context<GraphQuery> ctx) {
        return UnaryOperator.identity();
    }
}
