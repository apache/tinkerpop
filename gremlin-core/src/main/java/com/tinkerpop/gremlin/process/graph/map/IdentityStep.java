package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.Reversible;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityStep<S> extends MapStep<S, S> implements Reversible {

    public IdentityStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(Traverser::get);
    }
}
