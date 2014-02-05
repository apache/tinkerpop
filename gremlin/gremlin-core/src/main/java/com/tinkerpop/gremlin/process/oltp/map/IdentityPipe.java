package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Holder;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityPipe<S> extends MapPipe<S, S> {

    public IdentityPipe(final Traversal pipeline) {
        super(pipeline, Holder::get);
    }
}
