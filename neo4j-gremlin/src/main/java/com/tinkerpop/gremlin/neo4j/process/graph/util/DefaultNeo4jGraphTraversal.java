package com.tinkerpop.gremlin.neo4j.process.graph.util;

import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jGraphTraversal;
import com.tinkerpop.gremlin.neo4j.process.graph.step.map.Neo4jCypherStep;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultNeo4jGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> implements Neo4jGraphTraversal<S, E> {

    private final Neo4jGraph graph;

    public DefaultNeo4jGraphTraversal(final Class emanatingClass, final Neo4jGraph neo4jGraph) {
        super(emanatingClass);
        this.graph = neo4jGraph;
    }

    public <E2> Neo4jGraphTraversal<S, Map<String, E2>> cypher(final String query, final Map<String, Object> parameters) {
        return this.addStep(new Neo4jCypherStep<>(this, this.graph, query, parameters));
    }

    @Override
    public <E2> Neo4jGraphTraversal<S, E2> addStep(final Step<?, E2> step) {
        if (this.getTraversalEngine().isPresent()) throw Exceptions.traversalIsLocked();
        TraversalHelper.insertStep(step, this);
        return (Neo4jGraphTraversal) this;
    }
}
