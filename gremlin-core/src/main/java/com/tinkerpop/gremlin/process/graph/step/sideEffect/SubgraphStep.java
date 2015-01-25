package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectRegistrar;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.GraphFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A side-effect step that produces an edge induced subgraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SubgraphStep extends SideEffectStep<Edge> implements SideEffectCapable, SideEffectRegistrar, Reversible {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.OBJECT,
            TraverserRequirement.SIDE_EFFECTS
    ));

    private Graph subgraph;
    private String sideEffectKey;

    private static final Map<String, Object> DEFAULT_CONFIGURATION = new HashMap<String, Object>() {{
        put(Graph.GRAPH, "com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph"); // hard coded because TinkerGraph is not part of gremlin-core
    }};

    // TODO: add support for side-effecting out an edge list.

    public SubgraphStep(final Traversal traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.setConsumer(traverser -> {
            if (null == this.subgraph) {
                this.subgraph = traverser.sideEffects(this.sideEffectKey);
                if (!this.subgraph.features().vertex().supportsUserSuppliedIds() || !this.subgraph.features().edge().supportsUserSuppliedIds())
                    throw new IllegalArgumentException("The provided subgraph must support user supplied ids for vertices and edges: " + this.subgraph);
            }
            SubgraphStep.addEdgeToSubgraph(this.subgraph, traverser.get());
        });
    }

    @Override
    public void registerSideEffects() {
        if (null == this.sideEffectKey) this.sideEffectKey = this.getId();
        this.getTraversal().asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, () -> GraphFactory.open(DEFAULT_CONFIGURATION));
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
    public SubgraphStep clone() throws CloneNotSupportedException {
        final SubgraphStep clone = (SubgraphStep) super.clone();
        this.subgraph = null;
        return clone;
    }

    ///

    private static Vertex getOrCreate(final Graph subgraph, final Vertex vertex) {
        final Iterator<Vertex> vertexIterator = subgraph.iterators().vertexIterator(vertex.id());
        if (vertexIterator.hasNext()) return vertexIterator.next();
        final Vertex subgraphVertex = subgraph.addVertex(T.id, vertex.id(), T.label, vertex.label());
        vertex.iterators().propertyIterator().forEachRemaining(vertexProperty -> {
            final VertexProperty<?> subgraphVertexProperty = subgraphVertex.property(vertexProperty.key(), vertexProperty.value(), T.id, vertexProperty.id()); // TODO: demand vertex property id?
            vertexProperty.iterators().propertyIterator().forEachRemaining(property -> subgraphVertexProperty.<Object>property(property.key(), property.value()));
        });
        return subgraphVertex;
    }

    private static void addEdgeToSubgraph(final Graph subgraph, final Edge edge) {
        final Iterator<Edge> edgeIterator = subgraph.iterators().edgeIterator(edge.id());
        if (edgeIterator.hasNext()) return;
        final Iterator<Vertex> vertexIterator = edge.iterators().vertexIterator(Direction.BOTH);
        final Vertex subGraphOutVertex = getOrCreate(subgraph, vertexIterator.next());
        final Vertex subGraphInVertex = getOrCreate(subgraph, vertexIterator.next());
        final Edge subGraphEdge = subGraphOutVertex.addEdge(edge.label(), subGraphInVertex, T.id, edge.id());
        edge.iterators().propertyIterator().forEachRemaining(property -> subGraphEdge.<Object>property(property.key(), property.value()));
    }
}
