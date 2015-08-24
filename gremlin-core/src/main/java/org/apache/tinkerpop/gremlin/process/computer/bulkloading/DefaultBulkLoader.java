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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class DefaultBulkLoader implements BulkLoader {

    public final static String USE_USER_SUPPLIED_IDS_CFG_KEY = "use-user-supplied-ids";
    public final static String STORE_ORIGINAL_IDS_CFG_KEY = "store-original-ids";

    private boolean storeOriginalIds = false;
    private boolean useUserSuppliedIds = false;

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex getOrCreateVertex(final Vertex vertex, final Graph graph, final GraphTraversalSource g) {
        if (useUserSuppliedIds()) {
            return graph.addVertex(T.id, vertex.id(), T.label, vertex.label());
        }
        final Vertex v = graph.addVertex(T.label, vertex.label());
        if (storeOriginalIds()) {
            v.property(BulkLoaderVertexProgram.BULK_LOADER_VERTEX_ID, vertex.id());
        }
        return v;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Edge getOrCreateEdge(final Edge edge, final Vertex outVertex, final Vertex inVertex, final Graph graph, final GraphTraversalSource g) {
        final Edge e = outVertex.addEdge(edge.label(), inVertex);
        edge.properties().forEachRemaining(property -> e.property(property.key(), property.value()));
        return e;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VertexProperty getOrCreateVertexProperty(final VertexProperty<?> property, final Vertex vertex, final Graph graph, final GraphTraversalSource g) {
        final VertexProperty<?> vp = vertex.property(property.key(), property.value());
        vp.properties().forEachRemaining(metaProperty -> property.property(metaProperty.key(), metaProperty.value()));
        return vp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex getVertex(final Vertex vertex, final Graph graph, final GraphTraversalSource g) {
        return useUserSuppliedIds()
                ? getVertexById(vertex.id(), graph, g)
                : g.V().has(vertex.label(), BulkLoaderVertexProgram.BULK_LOADER_VERTEX_ID, vertex.id()).next();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean useUserSuppliedIds() {
        return useUserSuppliedIds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean storeOriginalIds() {
        return storeOriginalIds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Configuration configuration) {
        if (configuration.containsKey(USE_USER_SUPPLIED_IDS_CFG_KEY)) {
            useUserSuppliedIds = configuration.getBoolean(USE_USER_SUPPLIED_IDS_CFG_KEY);
        }
        if (configuration.containsKey(STORE_ORIGINAL_IDS_CFG_KEY)) {
            storeOriginalIds = configuration.getBoolean(STORE_ORIGINAL_IDS_CFG_KEY);
        }
    }
}
