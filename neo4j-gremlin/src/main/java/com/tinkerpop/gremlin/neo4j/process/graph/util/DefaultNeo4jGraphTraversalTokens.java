package com.tinkerpop.gremlin.neo4j.process.graph.util;

import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jGraphTraversal;
import com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum DefaultNeo4jGraphTraversalTokens implements AnonymousGraphTraversal {

    __;

    public <S> Neo4jGraphTraversal<S, S> start() {
        return new DefaultNeo4jGraphTraversal<>();
    }

}
