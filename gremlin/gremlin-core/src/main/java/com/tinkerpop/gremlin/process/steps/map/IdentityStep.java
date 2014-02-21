package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityStep<S> extends MapStep<S, S> {

    public IdentityStep(final Traversal traversal) {
        super(traversal, Holder::get);
    }
}
