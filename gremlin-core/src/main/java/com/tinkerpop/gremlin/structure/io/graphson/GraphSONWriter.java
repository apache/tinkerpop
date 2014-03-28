package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

/**
 * A @{link GraphWriter} that writes a graph and its elements to a JSON-based representation. This implementation
 * only supports JSON data types and is therefore lossy with respect to data types (e.g. a float will become a double).
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONWriter implements GraphWriter {
    private final ObjectMapper mapper;

    private GraphSONWriter(final ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void writeGraph(final OutputStream outputStream, final Graph g) throws IOException {
        this.mapper.writeValue(outputStream, new GraphSONGraph(g));
    }

    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v, final Direction direction) throws IOException {
        this.mapper.writeValue(outputStream, new GraphSONVertex(v, direction));
    }

    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v) throws IOException {
        this.mapper.writeValue(outputStream, v);
    }

    @Override
    public void writeEdge(final OutputStream outputStream, final Edge e) throws IOException {
        this.mapper.writeValue(outputStream, e);
    }

    public static Builder create() {
        return new Builder();
    }

    public static class Builder {
        private ObjectMapper mapper = new GraphSONObjectMapper();

        private Builder() {}

        public Builder customSerializer(final SimpleModule module) {
            this.mapper = new GraphSONObjectMapper(
                    Optional.ofNullable(module).orElseThrow(IllegalArgumentException::new));
            return this;
        }

        public GraphSONWriter build() {
            return new GraphSONWriter(mapper);
        }
    }
}
