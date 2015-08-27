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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class IncrementalBulkLoader extends DefaultBulkLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncrementalBulkLoader.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex getOrCreateVertex(final Vertex vertex, final Graph graph, final GraphTraversalSource g) {
        final Iterator<Vertex> iterator = useUserSuppliedIds()
                ? graph.vertices(vertex.id())
                : g.V().has(vertex.label(), getVertexIdProperty(), vertex.id());
        return iterator.hasNext() ? iterator.next() : super.getOrCreateVertex(vertex, graph, g);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Edge getOrCreateEdge(final Edge edge, final Vertex outVertex, final Vertex inVertex, final Graph graph, final GraphTraversalSource g) {
        final Traversal<Vertex, Edge> t = g.V(outVertex).outE(edge.label()).filter(__.inV().is(inVertex));
        if (t.hasNext()) {
            final Edge e = t.next();
            edge.properties().forEachRemaining(property -> {
                final Property<?> existing = e.property(property.key());
                if (!existing.isPresent() || !existing.value().equals(property.value())) {
                    e.property(property.key(), property.value());
                }
            });
            return e;
        }
        return super.getOrCreateEdge(edge, outVertex, inVertex, graph, g);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VertexProperty getOrCreateVertexProperty(final VertexProperty<?> property, final Vertex vertex, final Graph graph, final GraphTraversalSource g) {
        final VertexProperty<?> vp;
        final VertexProperty<?> existing = vertex.property(property.key());
        if (!existing.isPresent() || !existing.value().equals(property.value())) {
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
    public boolean storeOriginalIds() {
        return !useUserSuppliedIds();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Configuration configuration) {
        super.configure(configuration);
        if (configuration.containsKey(STORE_ORIGINAL_IDS_CFG_KEY)) {
            LOGGER.warn("{} automatically determines whether original identifiers should be stored or not, hence the " +
                    "configuration setting '{}' will be ignored.", this.getClass(), STORE_ORIGINAL_IDS_CFG_KEY);
        }
    }
}
