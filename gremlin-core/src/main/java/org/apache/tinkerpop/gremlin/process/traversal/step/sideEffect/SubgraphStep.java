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

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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

    private static final Map<String, Object> DEFAULT_CONFIGURATION = new HashMap<String, Object>() {{
        put(Graph.GRAPH, "org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph"); // hard coded because TinkerGraph is not part of gremlin-core
    }};

    // TODO: add support for side-effecting out an edge list.

    public SubgraphStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.getTraversal().asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, () -> GraphFactory.open(DEFAULT_CONFIGURATION));
    }

    @Override
    protected void sideEffect(final Traverser.Admin<Edge> traverser) {
        if (null == this.subgraph) {
            this.subgraph = traverser.sideEffects(this.sideEffectKey);
            if (!this.subgraph.features().vertex().supportsUserSuppliedIds() || !this.subgraph.features().edge().supportsUserSuppliedIds())
                throw new IllegalArgumentException("The provided subgraph must support user supplied ids for vertices and edges: " + this.subgraph);
        }
        SubgraphStep.addEdgeToSubgraph(this.subgraph, traverser.get());
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey);
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

    ///

    private static Vertex getOrCreate(final Graph subgraph, final Vertex vertex) {
        final Iterator<Vertex> vertexIterator = subgraph.vertices(vertex.id());
        if (vertexIterator.hasNext()) return vertexIterator.next();
        final Vertex subgraphVertex = subgraph.addVertex(T.id, vertex.id(), T.label, vertex.label());
        vertex.properties().forEachRemaining(vertexProperty -> {
            final VertexProperty<?> subgraphVertexProperty = subgraphVertex.property(vertexProperty.key(), vertexProperty.value(), T.id, vertexProperty.id()); // TODO: demand vertex property id?
            vertexProperty.properties().forEachRemaining(property -> subgraphVertexProperty.<Object>property(property.key(), property.value()));
        });
        return subgraphVertex;
    }

    private static void addEdgeToSubgraph(final Graph subgraph, final Edge edge) {
        final Iterator<Edge> edgeIterator = subgraph.edges(edge.id());
        if (edgeIterator.hasNext()) return;
        final Iterator<Vertex> vertexIterator = edge.vertices(Direction.BOTH);
        final Vertex subGraphOutVertex = getOrCreate(subgraph, vertexIterator.next());
        final Vertex subGraphInVertex = getOrCreate(subgraph, vertexIterator.next());
        final Edge subGraphEdge = subGraphOutVertex.addEdge(edge.label(), subGraphInVertex, T.id, edge.id());
        edge.properties().forEachRemaining(property -> subGraphEdge.<Object>property(property.key(), property.value()));
    }
}
