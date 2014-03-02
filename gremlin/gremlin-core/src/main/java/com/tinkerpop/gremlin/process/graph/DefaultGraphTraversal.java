package com.tinkerpop.gremlin.process.graph;

import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.map.StartStep;
import com.tinkerpop.gremlin.process.graph.util.optimizers.DedupOptimizer;
import com.tinkerpop.gremlin.process.graph.util.optimizers.IdentityOptimizer;
import com.tinkerpop.gremlin.process.graph.util.optimizers.SideEffectCapOptimizer;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultGraphTraversal<S, E> extends DefaultTraversal<S, E> implements GraphTraversal<S, E> {

    public DefaultGraphTraversal() {
        super();
        this.optimizers.register(new DedupOptimizer());
        this.optimizers.register(new IdentityOptimizer());
        this.optimizers.register(new SideEffectCapOptimizer());
    }

    public GraphTraversal<S, E> submit(final TraversalEngine engine) {
        final GraphTraversal<S, E> traversal = new DefaultGraphTraversal<>();
        traversal.addStep(new StartStep<>(traversal, engine.execute(this)));
        return traversal;
    }
}
