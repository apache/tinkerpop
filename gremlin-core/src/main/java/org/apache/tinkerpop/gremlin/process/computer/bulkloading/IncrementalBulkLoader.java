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
package org.apache.tinkerpop.gremlin.process.computer.bulkloading;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class IncrementalBulkLoader implements BulkLoader {

    private String bulkLoaderVertexId = BulkLoaderVertexProgram.DEFAULT_BULK_LOADER_VERTEX_ID;
    private boolean keepOriginalIds = true;
    private boolean userSuppliedIds = false;

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex getOrCreateVertex(final Vertex vertex, final Graph graph, final GraphTraversalSource g) {
        final Iterator<Vertex> iterator = useUserSuppliedIds()
                ? g.V().hasId(vertex.id())
                : g.V().has(vertex.label(), getVertexIdProperty(), vertex.id().toString());
        return iterator.hasNext()
                ? iterator.next()
                : useUserSuppliedIds()
                ? g.addV(vertex.label()).property(T.id, vertex.id()).next()
                : g.addV(vertex.label()).property(getVertexIdProperty(), vertex.id().toString()).next();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Edge getOrCreateEdge(final Edge edge, final Vertex outVertex, final Vertex inVertex, final Graph graph, final GraphTraversalSource g) {
        final Edge e;
        final Traversal<Vertex, Edge> t = g.V(outVertex).outE(edge.label()).filter(__.inV().is(inVertex));
        if (t.hasNext()) {
            e = t.next();
            edge.properties().forEachRemaining(property -> {
                final Property<?> existing = e.property(property.key());
                if (!existing.isPresent() || !existing.value().equals(property.value())) {
                    e.property(property.key(), property.value());
                }
            });
        } else {
            e = createEdge(edge, outVertex, inVertex, graph, g);
        }
        return e;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VertexProperty getOrCreateVertexProperty(final VertexProperty<?> property, final Vertex vertex, final Graph graph, final GraphTraversalSource g) {
        final VertexProperty<?> vp;
        final VertexProperty<?> existing = vertex.property(property.key());
        if (!existing.isPresent()) {
            return createVertexProperty(property, vertex, graph, g);
        }
        if (!existing.value().equals(property.value())) {
            vp = vertex.property(property.key(), property.value());
        } else {
            vp = existing;
        }
        property.properties().forEachRemaining(metaProperty -> {
            final Property<?> existing2 = vp.property(metaProperty.key());
            if (!existing2.isPresent() || !existing2.value().equals(metaProperty.value())) {
                vp.property(metaProperty.key(), metaProperty.value());
            }
        });
        return vp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex getVertex(final Vertex vertex, final Graph graph, final GraphTraversalSource g) {
        return useUserSuppliedIds()
                ? getVertexById(vertex.id(), graph, g)
                : g.V().has(vertex.label(), bulkLoaderVertexId, vertex.id()).next();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean useUserSuppliedIds() {
        return userSuppliedIds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean keepOriginalIds() {
        return keepOriginalIds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getVertexIdProperty() {
        return bulkLoaderVertexId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Configuration configuration) {
        if (configuration.containsKey(BulkLoaderVertexProgram.BULK_LOADER_VERTEX_ID_CFG_KEY)) {
            bulkLoaderVertexId = configuration.getString(BulkLoaderVertexProgram.BULK_LOADER_VERTEX_ID_CFG_KEY);
        }
        if (configuration.containsKey(BulkLoaderVertexProgram.USER_SUPPLIED_IDS_CFG_KEY)) {
            userSuppliedIds = configuration.getBoolean(BulkLoaderVertexProgram.USER_SUPPLIED_IDS_CFG_KEY);
        }
        if (configuration.containsKey(BulkLoaderVertexProgram.KEEP_ORIGINAL_IDS_CFG_KEY)) {
            keepOriginalIds = configuration.getBoolean(BulkLoaderVertexProgram.KEEP_ORIGINAL_IDS_CFG_KEY);
        }
    }
}
