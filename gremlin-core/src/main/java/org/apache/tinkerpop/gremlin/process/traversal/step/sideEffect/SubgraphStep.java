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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;

/**
 * A side-effect step that produces an edge induced subgraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SubgraphStep extends SideEffectStep<Edge> implements SideEffectCapable {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(
            TraverserRequirement.OBJECT,
            TraverserRequirement.SIDE_EFFECTS
    );

    private Graph subgraph;
    private String sideEffectKey;
    private Graph.Features.VertexFeatures parentGraphFeatures;
    private boolean subgraphSupportsMetaProperties = false;

    private static final Map<String, Object> DEFAULT_CONFIGURATION = new HashMap<String, Object>() {{
        put(Graph.GRAPH, "org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph"); // hard coded because TinkerGraph is not part of gremlin-core
    }};

    public SubgraphStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.getTraversal().asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, () -> GraphFactory.open(DEFAULT_CONFIGURATION));
    }

    @Override
    protected void sideEffect(final Traverser.Admin<Edge> traverser) {
        parentGraphFeatures = ((Graph) traversal.getGraph().get()).features().vertex();
        if (null == subgraph) {
            subgraph = traverser.sideEffects(sideEffectKey);
            if (!subgraph.features().vertex().supportsUserSuppliedIds() || !subgraph.features().edge().supportsUserSuppliedIds())
                throw new IllegalArgumentException("The provided subgraph must support user supplied ids for vertices and edges: " + this.subgraph);
        }

        subgraphSupportsMetaProperties = subgraph.features().vertex().supportsMetaProperties();

        addEdgeToSubgraph(traverser.get());
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.sideEffectKey);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public SubgraphStep clone() {
        final SubgraphStep clone = (SubgraphStep) super.clone();
        this.subgraph = null;
        return clone;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.sideEffectKey.hashCode();
    }

    ///

    private Vertex getOrCreate(final Vertex vertex) {
        final Iterator<Vertex> vertexIterator = subgraph.vertices(vertex.id());
        if (vertexIterator.hasNext()) return vertexIterator.next();
        final Vertex subgraphVertex = subgraph.addVertex(T.id, vertex.id(), T.label, vertex.label());

        vertex.properties().forEachRemaining(vertexProperty -> {
            final VertexProperty.Cardinality cardinality = parentGraphFeatures.getCardinality(vertexProperty.key());
            final VertexProperty<?> subgraphVertexProperty = subgraphVertex.property(cardinality, vertexProperty.key(), vertexProperty.value(), T.id, vertexProperty.id());

            // only iterate the VertexProperties if the current graph can have them and if the subgraph can support
            // them. unfortunately we don't have a way to write a test for this as we dont' have a graph that supports
            // user supplied ids and doesn't support metaproperties.
            if (parentGraphFeatures.supportsMetaProperties() && subgraphSupportsMetaProperties) {
                vertexProperty.properties().forEachRemaining(property -> subgraphVertexProperty.property(property.key(), property.value()));
            }
        });
        return subgraphVertex;
    }

    private void addEdgeToSubgraph(final Edge edge) {
        final Iterator<Edge> edgeIterator = subgraph.edges(edge.id());
        if (edgeIterator.hasNext()) return;
        final Iterator<Vertex> vertexIterator = edge.vertices(Direction.BOTH);
        final Vertex subGraphOutVertex = getOrCreate(vertexIterator.next());
        final Vertex subGraphInVertex = getOrCreate(vertexIterator.next());
        final Edge subGraphEdge = subGraphOutVertex.addEdge(edge.label(), subGraphInVertex, T.id, edge.id());
        edge.properties().forEachRemaining(property -> subGraphEdge.property(property.key(), property.value()));
    }
}
