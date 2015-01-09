package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.function.TriFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * A {@link GraphStrategy} implementation which enables mapper element IDs even for those graphs which don't
 * otherwise support them.
 * <p/>
 * For those graphs which support vertex indices but not edge indices (or vice versa), the strategy can be configured
 * to use mapper IDs only for vertices or only for edges.  ID generation is also configurable via ID {@link Supplier}
 * functions.
 * <p/>
 * If the {@link IdStrategy} is used in combination with a sequence of other strategies and when ID assignment
 * is enabled for an element, calls to strategies following this one are not made.  It is important to consider that
 * aspect of its operation when doing strategy composition.  Typically, the {@link IdStrategy} should be
 * executed last in a sequence.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class IdStrategy implements GraphStrategy {

    private final String idKey;

    private final Supplier<?> edgeIdSupplier;
    private final Supplier<?> vertexIdSupplier;

    private final boolean supportsVertexId;
    private final boolean supportsEdgeId;

    /**
     * Creates a new instance.  Public instantiation should be handled through the {@link Builder}.
     */
    private IdStrategy(final String idKey, final Supplier<?> vertexIdSupplier,
                       final Supplier<?> edgeIdSupplier, final boolean supportsVertexId,
                       final boolean supportsEdgeId) {
        this.idKey = idKey;
        this.edgeIdSupplier = edgeIdSupplier;
        this.vertexIdSupplier = vertexIdSupplier;
        this.supportsEdgeId = supportsEdgeId;
        this.supportsVertexId = supportsVertexId;
    }

    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return (f) -> (keyValues) -> {
            throwIfIdKeyIsSet(Vertex.class, ElementHelper.getKeys(keyValues));
            return f.apply(this.injectId(supportsVertexId, keyValues, vertexIdSupplier).toArray());
        };
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return (f) -> (label, v, keyValues) -> {
            throwIfIdKeyIsSet(Edge.class, ElementHelper.getKeys(keyValues));
            return f.apply(label, v, this.injectId(supportsEdgeId, keyValues, edgeIdSupplier).toArray());
        };
    }

    @Override
    public UnaryOperator<Function<Object[], Iterator<Edge>>> getGraphIteratorsEdgeIteratorStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return supportsVertexId ? (f) -> (ids) -> ctx.getStrategyGraph().getBaseGraph().E().has(idKey, Contains.within, Arrays.asList(ids)) : UnaryOperator.identity();
    }

    @Override
    public UnaryOperator<Function<Object[], Iterator<Vertex>>> getGraphIteratorsVertexIteratorStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return supportsVertexId ? (f) -> (ids) -> ctx.getStrategyGraph().getBaseGraph().V().has(idKey, Contains.within, Arrays.asList(ids)) : UnaryOperator.identity();
    }

    @Override
    public UnaryOperator<Function<Object[], GraphTraversal<Edge, Edge>>> getGraphEStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return supportsEdgeId ? (f) -> (ids) -> ctx.getStrategyGraph().getBaseGraph().E().has(idKey, Contains.within, Arrays.asList(ids)) : UnaryOperator.identity();
    }

    @Override
    public UnaryOperator<Function<Object[], GraphTraversal<Vertex, Vertex>>> getGraphVStrategy(final StrategyContext<StrategyGraph> ctx, final GraphStrategy composingStrategy) {
        return supportsVertexId ? (f) -> (ids) -> ctx.getStrategyGraph().getBaseGraph().V().has(idKey, Contains.within, Arrays.asList(ids)) : UnaryOperator.identity();
    }

    @Override
    public UnaryOperator<Supplier<Object>> getVertexIdStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return supportsVertexId ? (f) -> () -> ctx.getCurrent().getBaseVertex().value(idKey) : UnaryOperator.identity();
    }

    @Override
    public UnaryOperator<Supplier<Object>> getEdgeIdStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return supportsEdgeId ? (f) -> () -> ctx.getCurrent().getBaseEdge().value(idKey) : UnaryOperator.identity();
    }

    @Override
    public <V> UnaryOperator<BiFunction<String, V, VertexProperty<V>>> getVertexPropertyStrategy(final StrategyContext<StrategyVertex> ctx, final GraphStrategy composingStrategy) {
        return (f) -> (k, v) -> {
            throwIfIdKeyIsSet(ctx.getCurrent().getClass(), k);
            return f.apply(k, v);
        };
    }

    @Override
    public <V> UnaryOperator<BiFunction<String, V, Property<V>>> getEdgePropertyStrategy(final StrategyContext<StrategyEdge> ctx, final GraphStrategy composingStrategy) {
        return (f) -> (k, v) -> {
            throwIfIdKeyIsSet(ctx.getCurrent().getClass(), k);
            return f.apply(k, v);
        };
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyString(this);
    }

    private void throwIfIdKeyIsSet(final Class<? extends Element> element, final String k) {
        if (supportsAnId(element) && this.idKey.equals(k))
            throw new IllegalArgumentException(String.format("The key [%s] is protected by %s and cannot be set", idKey, IdStrategy.class.getSimpleName()));
    }

    private void throwIfIdKeyIsSet(final Class<? extends Element> element, final Set<String> keys) {
        if (supportsAnId(element) && keys.contains(this.idKey))
            throw new IllegalArgumentException(String.format("The key [%s] is protected by %s and cannot be set", idKey, IdStrategy.class.getSimpleName()));
    }

    private boolean supportsAnId(final Class<? extends Element> element) {
        return ((Vertex.class.isAssignableFrom(element) && supportsVertexId) || (Edge.class.isAssignableFrom(element) && supportsEdgeId));
    }

    /**
     * Gets the property name of the key used to lookup graph elements. Use this value to create an index in the underlying graph instance.
     */
    public String getIdKey() {
        return this.idKey;
    }

    public boolean isSupportsVertexId() {
        return supportsVertexId;
    }

    public boolean isSupportsEdgeId() {
        return supportsEdgeId;
    }

    private List<Object> injectId(final boolean supports, final Object[] keyValues, final Supplier<?> idMaker) {
        final List<Object> o = new ArrayList<>(Arrays.asList(keyValues));
        if (supports) {
            final Object val = ElementHelper.getIdValue(keyValues).orElse(idMaker.get());
            final int pos = o.indexOf(T.id);
            if (pos > -1) {
                o.remove(pos);
                o.remove(pos);
            }

            o.addAll(Arrays.asList(this.idKey, val));
        }

        return o;
    }

    /**
     * Create the {@link Builder} to create a {@link IdStrategy}.
     *
     * @param idKey The key to use for the index to lookup graph elements.
     */
    public static Builder build(final String idKey) {
        return new Builder(idKey);
    }

    public static final class Builder {
        private final String idKey;
        private Supplier<?> vertexIdSupplier;
        private Supplier<?> edgeIdSupplier;
        private boolean supportsVertexId;
        private boolean supportsEdgeId;


        private Builder(final String idKey) {
            this.idKey = idKey;
            this.edgeIdSupplier = this::supplyStringId;
            this.vertexIdSupplier = this::supplyStringId;
            this.supportsEdgeId = true;
            this.supportsVertexId = true;

        }

        public IdStrategy create() {
            if (!this.supportsEdgeId && !this.supportsVertexId)
                throw new IllegalStateException("Since supportsEdgeId and supportsVertexId are false, there is no need to use IdGraphStrategy");

            return new IdStrategy(this.idKey, this.vertexIdSupplier, this.edgeIdSupplier,
                    this.supportsVertexId, this.supportsEdgeId);
        }

        /**
         * Provide a function that will provide ids when none are provided explicitly when creating vertices. By default
         * a UUID string will be used if this value is not set.
         */
        public Builder vertexIdMaker(final Supplier<?> vertexIdSupplier) {
            if (null == vertexIdSupplier)
                throw new IllegalArgumentException("vertexIdSupplier");

            this.vertexIdSupplier = vertexIdSupplier;
            return this;
        }

        /**
         * Provide a function that will provide ids when none are provided explicitly when creating edges.  By default
         * a UUID string will be used if this value is not set.
         */
        public Builder edgeIdMaker(final Supplier<?> edgeIdSupplier) {
            if (null == edgeIdSupplier)
                throw new IllegalArgumentException("edgeIdSupplier");

            this.edgeIdSupplier = edgeIdSupplier;
            return this;
        }

        /**
         * Turn off support for this strategy for edges. Note that this value cannot be false if
         * {@link #supportsVertexId(boolean)} is also false.
         */
        public Builder supportsEdgeId(final boolean supports) {
            this.supportsEdgeId = supports;
            return this;
        }

        /**
         * Turn off support for this strategy for edges. Note that this value cannot be false if
         * {@link #supportsEdgeId(boolean)} is also false.
         */
        public Builder supportsVertexId(final boolean supports) {
            this.supportsVertexId = supports;
            return this;
        }

        private String supplyStringId() {
            return UUID.randomUUID().toString();
        }
    }
}
