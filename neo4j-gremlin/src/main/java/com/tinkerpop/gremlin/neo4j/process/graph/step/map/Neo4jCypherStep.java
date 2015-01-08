package com.tinkerpop.gremlin.neo4j.process.graph.step.map;

import com.tinkerpop.gremlin.neo4j.process.graph.step.util.Neo4jCypherIterator;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jHelper;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.ResourceIterator;

import java.util.Collections;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jCypherStep<S, E> extends FlatMapStep<S, Map<String, E>> {

    private static final String START = "start";

    public Neo4jCypherStep(final Traversal traversal, final Neo4jGraph graph, final String query, final Map<String, Object> parameters) {
        super(traversal);
        final ExecutionEngine cypher = Neo4jHelper.getCypher(graph);
        this.setFunction(traverser -> {
            final S s = traverser.get();
            parameters.put(START, s);
            final ExecutionResult result = cypher.execute(query, parameters);
            final ResourceIterator<Map<String, Object>> itty = result.iterator();
            return itty.hasNext() ? new Neo4jCypherIterator(itty, graph) : Collections.emptyIterator();
        });
    }
}
