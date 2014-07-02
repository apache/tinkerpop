package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SPredicate;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A side-effect step that produces an edge induced subgraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SubgraphStep<S> extends FilterStep<S> implements SideEffectCapable, PathConsumer, Reversible {
    private final Graph subgraph;
    private final Map<Object, Vertex> idMap;
    private final Set<Object> edgesAdded;

    // todo: add support for side-effecting out an edge list.

    public SubgraphStep(final Traversal traversal, final Graph subgraph,
                        final Set<Object> edgeIdHolder,
                        final Map<Object, Vertex> vertexMap,
                        final SPredicate<Edge> includeEdge) {
        super(traversal);
        this.edgesAdded = Optional.ofNullable(edgeIdHolder).orElse(new HashSet<>());
        this.idMap = Optional.ofNullable(vertexMap).orElse(new HashMap<>());
        this.subgraph = subgraph;
        this.traversal.memory().set(CAP_KEY, this.subgraph);
        this.setPredicate(traverser -> {
            traverser.getPath().stream().map(Pair::getValue1)
                    .filter(i -> i instanceof Edge)
                    .map(e -> (Edge) e)
                    .filter(e -> !edgesAdded.contains(e.id()))
                    .filter(includeEdge::test)
                    .forEach(e -> {
                        final Vertex newVOut = getOrCreateVertex(e.outV().next());
                        final Vertex newVIn = getOrCreateVertex(e.inV().next());
                        final Object[] edgeProps = getElementProperties(e);
                        newVOut.addEdge(e.label(), newVIn, edgeProps);
                        edgesAdded.add(e.id());
                    });
            return true;
        });
    }

    private Vertex getOrCreateVertex(final Vertex v) {
        final Vertex found;
        if (idMap.containsKey(v.id()))
            found = idMap.get(v.id());
        else {
            final Object[] vOutProps = getElementProperties(v);
            found = subgraph.addVertex(vOutProps);
            idMap.put(v.id(), found);
        }

        return found;
    }

    private Object[] getElementProperties(final Element e) {
        final Stream propertyStream = e.properties().entrySet().stream().flatMap(entry -> Stream.of(entry.getKey(), entry.getValue().value()));
        if (subgraph.getFeatures().vertex().supportsUserSuppliedIds())
            return Stream.concat(propertyStream, Stream.of(Element.ID, e.id())).toArray();
        else
            return propertyStream.toArray();
    }
}
