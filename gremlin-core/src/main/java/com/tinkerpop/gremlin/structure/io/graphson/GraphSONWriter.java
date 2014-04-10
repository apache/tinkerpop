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
        private boolean loadCustomSerializers = false;
        private boolean normalize = false;
        private SimpleModule custom = null;
        private GraphSONObjectMapper.TypeEmbedding typeEmbedding = GraphSONObjectMapper.TypeEmbedding.NONE;

        private Builder() {}

        /**
         * Specify a custom serializer module to handle types beyond those supported generally by TinkerPop.
         */
        public Builder customSerializer(final SimpleModule module) {
            this.custom = module;
            return this;
        }

        /**
         * Attempt to load external custom serialization modules from the class path.
         */
        public Builder loadCustomSerializers(final boolean loadCustomSerializers) {
            this.loadCustomSerializers = loadCustomSerializers;
            return this;
        }

        public Builder typeEmbedding(final GraphSONObjectMapper.TypeEmbedding typeEmbedding) {
            this.typeEmbedding = typeEmbedding;
            return this;
        }

        /**
         * Normalized output is deterministic with respect to the order of elements and properties in the resulting
         * XML document, and is compatible with line diff-based tools such as Git. Note: normalized output is
         * memory-intensive and is not appropriate for very large graphs.
         *
         * @param normalize whether to normalize the output.
         */
        public Builder normalize(final boolean normalize) {
            this.normalize = normalize;
            return this;
        }

        public GraphSONWriter build() {
            final GraphSONObjectMapper mapper = GraphSONObjectMapper.create()
                    .customSerializer(custom)
                    .loadCustomSerializers(loadCustomSerializers)
                    .normalize(normalize)
                    .typeEmbedding(typeEmbedding).build();
            return new GraphSONWriter(mapper);
        }
    }
}
