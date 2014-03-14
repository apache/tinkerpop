package com.tinkerpop.gremlin.algorithm.generator;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Triplet;

import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class AbstractGeneratorTest extends AbstractGremlinTest {

    protected boolean same(final Graph g1, final Graph g2) {
        return g1.V().toList().stream()
                .map(v -> Triplet.<Integer, List<Vertex>, List<Vertex>>with(v.getValue("oid"), v.in().toList(), v.out().toList()))
                .allMatch(p -> {
                    final Vertex v = (Vertex) g2.V().has("oid", p.getValue0()).next();
                    final boolean sameInVertices = sameInVertices(v, p.getValue1());
                    final boolean sameOutVertices = sameOutVertices(v, p.getValue2());
                    System.out.println(String.format("Comparison on IN %s OUT %s", sameInVertices, sameOutVertices));
                    return sameInVertices && sameOutVertices;
                });
    }

    private boolean sameInVertices(final Vertex v, final List<Vertex> inVertices) {
        return inVertices.stream()
                .allMatch(inVertex -> v.in().filter(hv -> hv.get().getValue("oid") == inVertex.getValue("oid")).hasNext());
    }

    private boolean sameOutVertices(final Vertex v, final List<Vertex> outVertices) {
        return outVertices.stream()
                .allMatch(outVertex -> v.out().filter(hv -> hv.get().getValue("oid") == outVertex.getValue("oid")).hasNext());
    }
}
