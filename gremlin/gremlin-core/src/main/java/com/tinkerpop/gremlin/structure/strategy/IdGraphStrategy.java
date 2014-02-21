package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.util.function.TriFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IdGraphStrategy implements GraphStrategy {

    private final String idKey;

    private final Supplier<?> edgeIdSupplier;
    private final Supplier<?> vertexIdSupplier;

    private final boolean supportsVertexId;
    private final boolean supportsEdgeId;

    private IdGraphStrategy(final String idKey, final Supplier<?> vertexIdSupplier,
                            final Supplier<?> edgeIdSupplier, final boolean supportsVertexId,
                            final boolean supportsEdgeId) {
        this.idKey = idKey;
        this.edgeIdSupplier = edgeIdSupplier;
        this.vertexIdSupplier = vertexIdSupplier;
        this.supportsEdgeId = supportsEdgeId;
        this.supportsVertexId = supportsVertexId;
    }

    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        return (f) -> (keyValues) -> f.apply(this.injectId(supportsVertexId, keyValues, vertexIdSupplier).toArray());
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.Context<StrategyWrappedVertex> ctx) {
        return (f) -> (label, v, keyValues) -> f.apply(label, v, this.injectId(supportsEdgeId, keyValues, edgeIdSupplier).toArray());
    }

    @Override
    public UnaryOperator<Function<Object, Vertex>> getGraphvStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        // don't apply f if supportsVertexId is true, because the implementation needs to be highjacked by the Strategy
        return supportsVertexId ? (f) -> (id) -> (Vertex) ctx.getBaseGraph().V().has(idKey, id).next() : UnaryOperator.identity();
    }

    @Override
    public UnaryOperator<Function<Object, Edge>> getGrapheStrategy(final Strategy.Context<StrategyWrappedGraph> ctx) {
        // don't apply f if supportsEdgeId is true, because the implementation needs to be highjacked by the Strategy
        return supportsEdgeId ? (f) -> (id) -> (Edge) ctx.getBaseGraph().E().has(idKey, id).next() : UnaryOperator.identity();
    }

    @Override
    public UnaryOperator<Supplier<Object>> getElementGetId(final Strategy.Context<? extends StrategyWrappedElement> ctx) {
        // don't apply f if ids are supported...need to return the id from the indexed key instead.
        // todo: think through terminal strategies like this one...how else can this be done?
        return ((ctx.getCurrent() instanceof Vertex) && supportsVertexId) || (ctx.getCurrent() instanceof Edge && supportsEdgeId) ?
                (f) -> () -> ctx.getCurrent().getBaseElement().getProperty(idKey).get() : UnaryOperator.identity();
    }

    /**
     * Gets the property name of the key used to lookup graph elements.  This is a "hidden" key created by
     * {@link Property.Key#hidden(String)}.  Use this value to create an index in the underlying graph instance.
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
            final int pos = o.indexOf(Element.ID);
            if (pos > -1) {
                o.remove(pos);
                o.remove(pos);
            }

            o.addAll(Arrays.asList(this.idKey, val));
        }

        return o;
    }

    public static final class Builder {
        private final String idKey;
        private Supplier<?> vertexIdSupplier;
        private Supplier<?> edgeIdSupplier;
        private boolean supportsVertexId;
        private boolean supportsEdgeId;
        private boolean hiddenIdKey;

        /**
         * Create the {@link Builder} to create a {@link IdGraphStrategy}.
         *
         * @param idKey The key to use for the index to lookup graph elements.
         */
        public Builder(final String idKey) {
            this.idKey = idKey;
            this.edgeIdSupplier = this::supplyStringId;
            this.vertexIdSupplier = this::supplyStringId;
            this.supportsEdgeId = true;
            this.supportsVertexId = true;
            this.hiddenIdKey = false;
        }

        public IdGraphStrategy build() {
            if (!this.supportsEdgeId && !this.supportsVertexId)
                throw new IllegalStateException("Since supportsEdgeId and supportsVertexId are false, there is no need to use IdGraphStrategy");

            final String keyForId = this.hiddenIdKey ? Property.Key.hidden(this.idKey) : this.idKey;
            return new IdGraphStrategy(keyForId, this.vertexIdSupplier, this.edgeIdSupplier,
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

        /**
         * Converts the key supplied to the constructor of the builder to a hidden key.
         */
        public Builder useHiddenIdKey(final boolean hidden) {
            this.hiddenIdKey = hidden;
            return this;
        }

        private String supplyStringId() {
            return UUID.randomUUID().toString();
        }
    }
}
