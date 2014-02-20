package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Strategy;
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
        // assumes this is an indexed key.
        this.idKey = Property.Key.hidden(idKey);

        this.edgeIdSupplier = edgeIdSupplier;
        this.vertexIdSupplier = vertexIdSupplier;
        this.supportsEdgeId = supportsEdgeId;
        this.supportsVertexId = supportsVertexId;
    }

    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context<Graph> ctx) {
        return (f) -> (keyValues) -> f.apply(this.injectId(supportsVertexId, keyValues, vertexIdSupplier).toArray());
    }

    @Override
    public UnaryOperator<TriFunction<String, Vertex, Object[], Edge>> getAddEdgeStrategy(final Strategy.Context<Vertex> ctx) {
        return (f) -> (label, v, keyValues) -> f.apply(label, v, this.injectId(supportsEdgeId, keyValues, edgeIdSupplier).toArray());
    }

    @Override
    public UnaryOperator<Function<Object, Vertex>> getGraphvStrategy(final Strategy.Context<Graph> ctx) {
        // don't apply f because the implementation needs to be highjacked by the Strategy
        if (supportsVertexId)
            return (f) -> (id) -> (Vertex) ctx.getGraph().V().has(idKey, id).next();
        else
            return UnaryOperator.identity();
    }

    @Override
    public UnaryOperator<Function<Object, Edge>> getGrapheStrategy(final Strategy.Context<Graph> ctx) {
        // don't apply f because the implementation needs to be highjacked by the Strategy
        return (f) -> (id) -> (Edge) ctx.getGraph().E().has(idKey, id).next();
    }

    @Override
    public UnaryOperator<Supplier<Object>> getElementGetId(final Strategy.Context<? extends Element> ctx) {
        // if the property is not present then it's likely an internal call from the graph on addVertex in which case,
        // the base implementation should be called
        return (f) -> () -> ctx.getCurrent().getProperty(idKey).orElse(f.get());
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

        public Builder(final String idKey) {
            this.idKey = idKey;
            this.edgeIdSupplier = this::supplyStringId;
            this.vertexIdSupplier = this::supplyStringId;
            this.supportsEdgeId = true;
            this.supportsVertexId = true;
        }

        public IdGraphStrategy build() {
            if (!this.supportsEdgeId && !this.supportsVertexId)
                throw new IllegalStateException("Since supportsEdgeId and supportsVertexId are false, there is no need to use IdGraphStrategy");

            return new IdGraphStrategy(this.idKey, this.vertexIdSupplier, this.edgeIdSupplier,
                    this.supportsVertexId, this.supportsEdgeId);
        }

        public Builder vertexIdMaker(final Supplier<?> vertexIdSupplier) {
            if (null == vertexIdSupplier)
                throw new IllegalArgumentException("vertexIdSupplier");

            this.vertexIdSupplier = vertexIdSupplier;
            return this;
        }

        public Builder edgeIdMaker(final Supplier<?> edgeIdSupplier) {
            if (null == edgeIdSupplier)
                throw new IllegalArgumentException("edgeIdSupplier");

            this.edgeIdSupplier = edgeIdSupplier;
            return this;
        }

        public Builder supportsEdgeId(final boolean supports) {
            this.supportsEdgeId = supports;
            return this;
        }

        public Builder supportsVertexId(final boolean supports) {
            this.supportsVertexId = supports;
            return this;
        }

        private String supplyStringId() {
            return UUID.randomUUID().toString();
        }
    }
}
