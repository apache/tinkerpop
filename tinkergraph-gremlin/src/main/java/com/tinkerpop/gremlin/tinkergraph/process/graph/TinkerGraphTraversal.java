package com.tinkerpop.gremlin.tinkergraph.process.graph;

import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    public GraphTraversal<S, E> submit(final TraversalEngine engine) {
        if (engine instanceof GraphComputer)
            TinkerHelper.prepareTraversalForComputer(this);
        return super.submit(engine);
    }
}
