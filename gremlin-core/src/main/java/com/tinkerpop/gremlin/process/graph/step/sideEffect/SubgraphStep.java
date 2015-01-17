package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.GraphFactory;
import org.javatuples.Pair;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A side-effect step that produces an edge induced subgraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class SubgraphStep<S> extends SideEffectStep<S> implements SideEffectCapable, Reversible {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.OBJECT,
            TraverserRequirement.SIDE_EFFECTS,
            TraverserRequirement.PATH
    ));


    private Graph subgraph;
    private Boolean subgraphSupportsUserIds;

    private final Map<Object, Vertex> idVertexMap;
    private final Set<Object> edgeIdsAdded;
    private final String sideEffectKey;
    private static final Map<String, Object> DEFAULT_CONFIGURATION = new HashMap<String, Object>() {{
        put("gremlin.graph", "com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph");
    }};

    // TODO: add support for side-effecting out an edge list.

    public SubgraphStep(final Traversal traversal, final String sideEffectKey,
                        final Set<Object> edgeIdHolder,
                        final Map<Object, Vertex> idVertexMap,
                        final Predicate<Edge> includeEdge) {
        super(traversal);
        this.sideEffectKey = null == sideEffectKey ? this.getLabel().orElse(this.getId()) : sideEffectKey;
        this.edgeIdsAdded = null == edgeIdHolder ? new HashSet<>() : edgeIdHolder;
        this.idVertexMap = null == idVertexMap ? new HashMap<>() : idVertexMap;
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, () -> GraphFactory.open(DEFAULT_CONFIGURATION));
        this.setConsumer(traverser -> {
            if (null == this.subgraph) {
                this.subgraph = traverser.asAdmin().getSideEffects().get(this.sideEffectKey);
                this.subgraphSupportsUserIds = this.subgraph.features().vertex().supportsUserSuppliedIds();
            }
            traverser.path().stream().map(Pair::getValue0)
                    .filter(i -> i instanceof Edge)
                    .map(e -> (Edge) e)
                    .filter(e -> !this.edgeIdsAdded.contains(e.id()))
                    .filter(includeEdge::test)
                    .forEach(e -> {
                        final Vertex newVOut = getOrCreateVertex(e.outV().next());
                        final Vertex newVIn = getOrCreateVertex(e.inV().next());
                        newVOut.addEdge(e.label(), newVIn, ElementHelper.getProperties(e, subgraphSupportsUserIds, false, Collections.emptySet()));
                        // TODO: If userSuppliedIds exist, don't do this to save sideEffects
                        this.edgeIdsAdded.add(e.id());
                    });
        });
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    private Vertex getOrCreateVertex(final Vertex vertex) {
        Vertex foundVertex = null;
        if (this.subgraphSupportsUserIds) {
            try {
                foundVertex = this.subgraph.V(vertex.id()).next();
            } catch (final NoSuchElementException e) {
                // do nothing;
            }
        } else {
            foundVertex = this.idVertexMap.get(vertex.id());
        }

        if (null == foundVertex) {
            foundVertex = this.subgraph.addVertex(ElementHelper.getProperties(vertex, this.subgraphSupportsUserIds, true, Collections.emptySet()));
            if (!this.subgraphSupportsUserIds)
                this.idVertexMap.put(vertex.id(), foundVertex);
        }
        return foundVertex;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }
}
