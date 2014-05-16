package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityStep<S> extends MapStep<S, S> {

    public IdentityStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(Traverser::get);
    }
}
