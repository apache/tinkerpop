package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Strategy;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IdGraphStrategy implements GraphStrategy {

    private final String idKey;

    private final Optional<Supplier<Object>> edgeIdSupplier;
    private final Optional<Supplier<Object>> vertexIdSupplier;

    private IdGraphStrategy(final String idKey, final Supplier<Object> vertexIdSupplier,
                            final Supplier<Object> edgeIdSupplier) {
        // assumes this is an indexed key.
        this.idKey = Property.Key.hidden(idKey);

        this.edgeIdSupplier = Optional.ofNullable(edgeIdSupplier);
        this.vertexIdSupplier = Optional.ofNullable(vertexIdSupplier);
    }

    @Override
    public UnaryOperator<Function<Object[], Vertex>> getAddVertexStrategy(final Strategy.Context<Graph> ctx) {
        return (f) -> (keyValues) -> {
            final List<Object> o = new ArrayList<>(Arrays.asList(keyValues));

            // todo: use IdFactory to generate an id if "null"
            o.addAll(Arrays.asList(this.idKey, ElementHelper.getIdValue(keyValues).orElse(null)));
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

    public static final class Builder {
        private final String idKey;

        public Builder(final String idKey) {
            this.idKey = idKey;
        }

        public IdGraphStrategy build() {
            return new IdGraphStrategy(this.idKey, null, null);
        }
    }
}
