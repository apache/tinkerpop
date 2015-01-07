package com.tinkerpop.gremlin.tinkergraph.process.graph.util;

import com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum DefaultTinkerGraphTraversalTokens implements AnonymousGraphTraversal {

    __;

    public <S> GraphTraversal<S, S> start() {
        return new DefaultTinkerGraphTraversal<>();
    }
}
