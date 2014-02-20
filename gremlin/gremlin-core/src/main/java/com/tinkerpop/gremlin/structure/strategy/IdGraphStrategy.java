package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Strategy;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

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

    private IdGraphStrategy(final String idKey, final Supplier<?> vertexIdSupplier,
                            final Supplier<?> edgeIdSupplier) {
        // assumes this is an indexed key.
        this.idKey = Property.Key.hidden(idKey);

        this.edgeIdSupplier = edgeIdSupplier;
        this.vertexIdSupplier = vertexIdSupplier;
    }

    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context<Graph> ctx) {
        return (f) -> (keyValues) -> {
            final List<Object> o = new ArrayList<>(Arrays.asList(keyValues));
            final Object val = ElementHelper.getIdValue(keyValues).orElse(vertexIdSupplier.get());
            final int pos = o.indexOf(Element.ID);
            if (pos > -1) {
                o.remove(pos);
                o.remove(pos);
            }

            o.addAll(Arrays.asList(this.idKey, val));
            return f.apply(o.toArray());
        };
    }

    @Override
    public UnaryOperator<Function<Object, Vertex>> getGraphvStrategy(final Strategy.Context<Graph> ctx) {
        // don't apply f because the implementation needs to be highjacked by the Strategy
        return (f) -> (id) -> (Vertex) ctx.getGraph().V().has(idKey, id).next();
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
        return (f) -> () -> ctx.getCurrent().getProperty(idKey).orElse(f);
    }

    public static final class Builder {
        private final String idKey;
        private Supplier<?> vertexIdSupplier;
        private Supplier<?> edgeIdSupplier;

        public Builder(final String idKey) {
            this.idKey = idKey;
            this.edgeIdSupplier = this::supplyStringId;
            this.vertexIdSupplier = this::supplyStringId;
        }

        public IdGraphStrategy build() {
            return new IdGraphStrategy(this.idKey, this.vertexIdSupplier, this.edgeIdSupplier);
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

        private String supplyStringId() {
            return UUID.randomUUID().toString();
        }
    }
}
