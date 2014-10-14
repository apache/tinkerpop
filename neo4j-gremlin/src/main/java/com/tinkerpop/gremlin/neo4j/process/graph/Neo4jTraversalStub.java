package com.tinkerpop.gremlin.neo4j.process.graph;

import com.tinkerpop.gremlin.neo4j.process.graph.step.map.Neo4jCypherStep;
import com.tinkerpop.gremlin.neo4j.process.graph.util.Neo4jGraphTraversal;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Element;

import java.util.Map;

/**
 * Neo4jTraversalStub is merged with {@link GraphTraversal} via the Maven exec-plugin.
 * The Maven plugin yields Neo4jTraversal which is ultimately what is executed at runtime.
 * This class maintains {@link Neo4jTraversal} specific methods that extends {@link GraphTraversal}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Neo4jTraversalStub<S, E> extends GraphTraversal<S, E> {

    public static <S> Neo4jTraversal<S, S> of(final Graph graph) {
        if (!(graph instanceof Neo4jGraph))
            throw new IllegalArgumentException(String.format("graph must be of type %s", Neo4jGraph.class));
        return new Neo4jGraphTraversal<>((Neo4jGraph) graph);
    }

    public static <S> Neo4jTraversal<S, S> of() {
        return new Neo4jGraphTraversal<>();
    }

    @Override
    public default <E2> Neo4jTraversal<S, E2> addStep(final Step<?, E2> step) {
        return (Neo4jTraversal) GraphTraversal.super.addStep((Step) step);
    }

    public default <E2> Neo4jTraversal<S, Map<String, E2>> cypher(final String query) {
        return (Neo4jTraversal) this.addStep(new Neo4jCypherStep<>(this, query));
    }

    public default <E2> Neo4jTraversal<S, Map<String, E2>> cypher(final String query, final Map<String, Object> parameters) {
        return (Neo4jTraversal) this.addStep(new Neo4jCypherStep<>(this, query, parameters));
    }
}