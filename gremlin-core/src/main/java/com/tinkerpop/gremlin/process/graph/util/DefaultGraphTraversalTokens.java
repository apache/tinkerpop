package com.tinkerpop.gremlin.process.graph.util;

import com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum DefaultGraphTraversalTokens implements AnonymousGraphTraversal {

    __;

    public <S> GraphTraversal<S, S> start() {
        return new DefaultGraphTraversal<>();
    }

}
