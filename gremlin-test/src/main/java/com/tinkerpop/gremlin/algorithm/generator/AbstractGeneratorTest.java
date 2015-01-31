package com.tinkerpop.gremlin.algorithm.generator;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.javatuples.Triplet;

import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class AbstractGeneratorTest extends AbstractGremlinTest {

    /**
     * Asserts that two graphs are the "same" in way of structure.  It assumes that the graphs both have vertices
     * with an "oid" property that is an Integer value.  It iterates each vertex in graph 1, using the "oid" to
     * lookup vertices in graph 2.  For each one found it checks both IN and OUT vertices to ensure that they
     * attach to the same IN/OUT vertices given their "oid" properties.
     */
    protected boolean same(final Graph g1, final Graph g2) {
        return StreamFactory.stream(g1.iterators().vertexIterator())
                .map(v -> Triplet.<Integer, List<Vertex>, List<Vertex>>with(v.value("oid"), v.in().toList(), v.out().toList()))
                .allMatch(p -> {
                    final Vertex v = (Vertex) g2.V().has("oid", p.getValue0()).next();
                    return sameInVertices(v, p.getValue1()) && sameOutVertices(v, p.getValue2());
                });
    }

    private boolean sameInVertices(final Vertex v, final List<Vertex> inVertices) {
        return inVertices.stream()
                .allMatch(inVertex -> v.in().filter(hv -> hv.get().value("oid").equals(inVertex.value("oid"))).hasNext());
    }

    private boolean sameOutVertices(final Vertex v, final List<Vertex> outVertices) {
        return outVertices.stream()
                .allMatch(outVertex -> v.out().filter(hv -> hv.get().value("oid").equals(outVertex.value("oid"))).hasNext());
    }
}
