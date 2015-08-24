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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class IncrementalBulkLoader extends DefaultBulkLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncrementalBulkLoader.class);

    @Override
    public Vertex getOrCreateVertex(final Vertex vertex, final Graph graph, final GraphTraversalSource g) {
        if (useUserSuppliedIds()) {
            try {
                return getVertexById(vertex.id(), graph, g);
            } catch (NoSuchElementException ignored) {
            }
        } else {
            final Traversal<Vertex, Vertex> t = g.V().has(vertex.label(), BulkLoaderVertexProgram.BULK_LOADER_VERTEX_ID, vertex.id());
            if (t.hasNext()) return t.next();
        }
        return super.getOrCreateVertex(vertex, graph, g);
    }

    @Override
    public Edge getOrCreateEdge(final Edge edge, final Vertex outVertex, final Vertex inVertex, final Graph graph, final GraphTraversalSource g) {
        final Traversal<Vertex, Edge> t = g.V(outVertex).outE(edge.label()).filter(__.inV().is(inVertex));
        if (t.hasNext()) {
            final Edge e = t.next();
            edge.properties().forEachRemaining(property -> e.property(property.key(), property.value()));
            return e;
        }
        return super.getOrCreateEdge(edge, outVertex, inVertex, graph, g);
    }

    @Override
    public boolean storeOriginalIds() {
        return !useUserSuppliedIds();
    }

    @Override
    public void configure(final Configuration configuration) {
        super.configure(configuration);
        if (configuration.containsKey("store-original-ids")) {
            LOGGER.warn("{} automatically determines whether original identifiers should be stored or not, hence the " +
                    "configuration setting 'store-original-ids' will be ignored.", this.getClass());
        }
    }
}
