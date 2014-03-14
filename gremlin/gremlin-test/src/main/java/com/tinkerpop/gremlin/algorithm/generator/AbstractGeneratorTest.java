package com.tinkerpop.gremlin.algorithm.generator;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Triplet;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class AbstractGeneratorTest extends AbstractGremlinTest {
    protected boolean same(final Graph g1, final Graph g2) {
        return g1.V().toList().stream()
                .map(v -> Triplet.with(v.getValue("oid"), v.inE().count(), v.outE().count()))
                .allMatch(p -> {
                    final Vertex v = (Vertex) g2.V().has("oid", p.getValue0()).next();
                    return p.getValue1() == v.inE().count()
                            && p.getValue2() == v.outE().count();
                });
    }
}
