package com.tinkerpop.gremlin.neo4j.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.neo4j.process.graph.step.util.Neo4jCypherIterator;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import org.neo4j.graphdb.ResourceIterator;

import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jCypherStartStep<E> extends StartStep<Map<String, E>> {

    public Neo4jCypherStartStep(final ResourceIterator<Map<String, E>> itty, final Traversal traversal, final Neo4jGraph graph) {
        super(traversal, new Neo4jCypherIterator<>(itty, graph));
    }
}
