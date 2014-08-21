package com.tinkerpop.gremlin.neo4j.process.graph.step.map;

import com.tinkerpop.gremlin.neo4j.process.graph.step.util.Neo4jCypherIterator;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.ResourceIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jCypherStep<S, E> extends FlatMapStep<S, Map<String, E>> {

    private static final String START = "start";

    public Neo4jCypherStep(final String query, final Traversal traversal) {
        this(query, new HashMap<>(), traversal);
    }

    public Neo4jCypherStep(final String query, final Map<String, Object> params, final Traversal traversal) {
        super(traversal);
        final Neo4jGraph graph = (Neo4jGraph) traversal.sideEffects().getGraph();
        final ExecutionEngine cypher = graph.getCypher();
        this.setFunction(traverser -> {
            final S s = traverser.get();
            params.put(START, s);
            final ExecutionResult result = cypher.execute(query, params);
            final ResourceIterator<Map<String, Object>> itty = result.iterator();
            return itty.hasNext() ? new Neo4jCypherIterator(itty, graph) : new ArrayList().iterator();
        });
    }
}
