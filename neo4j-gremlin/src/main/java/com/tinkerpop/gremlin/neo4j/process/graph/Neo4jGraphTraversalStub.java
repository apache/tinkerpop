package com.tinkerpop.gremlin.neo4j.process.graph;

import com.tinkerpop.gremlin.neo4j.process.graph.step.map.Neo4jCypherStep;
import com.tinkerpop.gremlin.neo4j.process.graph.util.DefaultNeo4jGraphTraversal;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Element;

import java.util.Map;

/**
 * Neo4jGraphTraversalStub is merged with {@link GraphTraversal} via the Maven exec-plugin.
 * The Maven plugin yields Neo4jGraphTraversal which is ultimately what is depended on by user source.
 * This class maintains {@link Neo4jGraphTraversal} specific methods that extend {@link GraphTraversal}.
 * Adding {@link Element} to the JavaDoc so it sticks in the import during "optimize imports" code cleaning.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
interface Neo4jGraphTraversalStub<S, E> extends GraphTraversal.Admin<S, E>, GraphTraversal<S, E> {

    public static <S> Neo4jGraphTraversal<S, S> of(final Graph graph) {
        if (!(graph instanceof Neo4jGraph))
            throw new IllegalArgumentException(String.format("graph must be of type %s", Neo4jGraph.class));
        return new DefaultNeo4jGraphTraversal<>((Neo4jGraph) graph);
    }

    public static <S> Neo4jGraphTraversal<S, S> of() {
        return new DefaultNeo4jGraphTraversal<>();
    }

    @Override
    public default <E2> Neo4jGraphTraversal<S, E2> addStep(final Step<?, E2> step) {
        return (Neo4jGraphTraversal) GraphTraversal.Admin.super.addStep((Step) step);
    }

    public default <E2> Neo4jGraphTraversal<S, Map<String, E2>> cypher(final String query) {
        return (Neo4jGraphTraversal) this.addStep(new Neo4jCypherStep<>(this, query));
    }

    public default <E2> Neo4jGraphTraversal<S, Map<String, E2>> cypher(final String query, final Map<String, Object> parameters) {
        return (Neo4jGraphTraversal) this.addStep(new Neo4jCypherStep<>(this, query, parameters));
    }
}