/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.EdgeScanOrder;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.EdgeSink;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.PropertySource;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.PropertyVisitor;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.SnapshotSource;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.SourceDefaults;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.SourceVersion;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.UnsupportedSnapshotDataException;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.VertexPropertySource;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.VertexPropertyVisitor;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.VertexSink;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A {@link SnapshotSource} that streams a Gryo file, as written by {@code g.io(file.kryo).write()}, one star graph at a
 * time. The file is never loaded into a graph: {@link #scanVertices} reads it once and {@link #scanEdges} reads it a
 * second time, so at most one star graph is held in memory.
 * <p/>
 * Every vertex label, every vertex property (all values of a key, in file order, each with its identifier and its
 * meta-properties) and null values are emitted. Gryo graph files carry no graph variables, so {@link #scanVariables}
 * emits nothing. Edges are emitted from the out-edges of each star graph, in the order of the vertices, so
 * {@link #edgeScanOrder()} is {@link EdgeScanOrder#GROUPED_BY_OUT_VERTEX}. The in-edges of each star graph are ignored.
 * Data the snapshot cannot represent is rejected with {@link UnsupportedSnapshotDataException} before the element
 * reaches the sink: vertices with a null label and null vertex property identifiers.
 * <p/>
 * The file does not record the graph's defaults, so {@link #defaults()} reports {@link SourceDefaults#DEFAULT} unless
 * other defaults are supplied. Vertex and edge ordinals follow the order of the file, which is not necessarily the
 * iteration order of a {@link TinkerGraph} that loaded the same file.
 */
public final class GryoSnapshotSource implements SnapshotSource {

    private final Path file;
    private final GryoMapper mapper;
    private final SourceDefaults defaults;

    /**
     * Reads the file with a mapper configured like the one {@code io()} uses for {@code .kryo} files: the default Gryo
     * version, no native Java serialization and the {@link TinkerIoRegistryV3}.
     */
    public GryoSnapshotSource(final String file) {
        this(Paths.get(file));
    }

    /**
     * Reads the file with a mapper configured like the one {@code io()} uses for {@code .kryo} files: the default Gryo
     * version, no native Java serialization and the {@link TinkerIoRegistryV3}.
     */
    public GryoSnapshotSource(final Path file) {
        this(file, defaultMapper(), SourceDefaults.DEFAULT);
    }

    public GryoSnapshotSource(final Path file, final GryoMapper mapper) {
        this(file, mapper, SourceDefaults.DEFAULT);
    }

    /**
     * @param file     the Gryo file
     * @param mapper   the mapper the file was written with
     * @param defaults the defaults to record in the manifest
     * @throws IllegalArgumentException if the file does not exist
     */
    public GryoSnapshotSource(final Path file, final GryoMapper mapper, final SourceDefaults defaults) {
        this.file = Objects.requireNonNull(file).toAbsolutePath();
        this.mapper = Objects.requireNonNull(mapper);
        this.defaults = Objects.requireNonNull(defaults);
        if (!Files.isRegularFile(this.file))
            throw new IllegalArgumentException("Gryo file does not exist: " + this.file);
    }

    /**
     * The mapper {@code io()} builds for {@code .kryo} files plus the TinkerGraph registry.
     */
    public static GryoMapper defaultMapper() {
        return GryoMapper.build().javaSerializationAllowed(false).addRegistry(TinkerIoRegistryV3.instance()).create();
    }

    /**
     * The identifier is {@code gryo:} and the absolute path. The version is the file size and last modified time.
     */
    @Override
    public SourceVersion version() {
        try {
            return new SourceVersion("gryo:" + file, "size=" + Files.size(file) +
                    ";modified=" + Files.getLastModifiedTime(file).toMillis());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public EdgeScanOrder edgeScanOrder() {
        return EdgeScanOrder.GROUPED_BY_OUT_VERTEX;
    }

    @Override
    public SourceDefaults defaults() {
        return defaults;
    }

    @Override
    public void scanVertices(final VertexSink sink) {
        final MutableVertexProperties vertexProperties = new MutableVertexProperties();
        final List<String> labels = new ArrayList<>();
        scan(vertex -> {
            labels.clear();
            for (final String label : vertex.labels()) {
                if (null == label)
                    throw new UnsupportedSnapshotDataException(String.format("vertex [%s] has a null label", vertex.id()));
                labels.add(label);
            }
            validateVertexProperties(vertex);
            vertexProperties.vertex = vertex;
            sink.vertex(vertex.id(), labels, vertexProperties);
            vertexProperties.vertex = null;
        });
    }

    @Override
    public void scanEdges(final EdgeSink sink) {
        final MutableEdgeProperties edgeProperties = new MutableEdgeProperties();
        scan(vertex -> {
            final Iterator<Edge> edges = vertex.edges(Direction.OUT);
            while (edges.hasNext()) {
                final Edge edge = edges.next();
                if (null == edge.label())
                    throw new UnsupportedSnapshotDataException(String.format("edge [%s] has a null label", edge.id()));
                edgeProperties.edge = edge;
                sink.edge(edge.id(), edge.label(), edge.outVertex().id(), edge.inVertex().id(), edgeProperties);
            }
            edgeProperties.edge = null;
        });
    }

    private interface StarVertexConsumer {
        void accept(Vertex vertex);
    }

    /**
     * One pass over the file. The vertices are star graphs that are valid only during the consumer call.
     */
    private void scan(final StarVertexConsumer consumer) {
        final GryoReader reader = GryoReader.build().mapper(mapper).create();
        try (InputStream in = Files.newInputStream(file)) {
            final Iterator<Vertex> vertices = reader.readVertices(in, attachable -> attachable.get(), null, null);
            while (vertices.hasNext()) {
                consumer.accept(vertices.next());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Checks the vertex property identifiers up front so that unsupported data fails the scan even if the sink never
     * visits the properties.
     */
    private static void validateVertexProperties(final Vertex vertex) {
        final Iterator<VertexProperty<Object>> properties = vertex.properties();
        while (properties.hasNext()) {
            final VertexProperty<Object> vp = properties.next();
            if (null == vp.id())
                throw new UnsupportedSnapshotDataException(String.format(
                        "Vertex property '%s' of vertex [%s] has a null identifier", vp.key(), vertex.id()));
        }
    }

    /**
     * Reused for every vertex, as the contract allows because it is valid only during the sink call. Its content has
     * already been validated.
     */
    private static final class MutableVertexProperties implements VertexPropertySource {

        private Vertex vertex;
        private final MutableMetaProperties metaProperties = new MutableMetaProperties();

        @Override
        public void forEach(final VertexPropertyVisitor visitor) {
            final Iterator<VertexProperty<Object>> properties = vertex.properties();
            while (properties.hasNext()) {
                final VertexProperty<Object> vp = properties.next();
                metaProperties.vertexProperty = vp;
                visitor.vertexProperty(vp.id(), vp.key(), vp.value(), metaProperties);
            }
            metaProperties.vertexProperty = null;
        }
    }

    /**
     * Reused for every vertex property, as the contract allows because it is valid only during the visitor call.
     */
    private static final class MutableMetaProperties implements PropertySource {

        private VertexProperty<Object> vertexProperty;

        @Override
        public void forEach(final PropertyVisitor visitor) {
            final Iterator<Property<Object>> properties = vertexProperty.properties();
            while (properties.hasNext()) {
                final Property<Object> property = properties.next();
                visitor.property(property.key(), property.value());
            }
        }
    }

    /**
     * Reused for every edge, as the contract allows because it is valid only during the sink call.
     */
    private static final class MutableEdgeProperties implements PropertySource {

        private Edge edge;

        @Override
        public void forEach(final PropertyVisitor visitor) {
            final Iterator<Property<Object>> properties = edge.properties();
            while (properties.hasNext()) {
                final Property<Object> property = properties.next();
                visitor.property(property.key(), property.value());
            }
        }
    }
}
