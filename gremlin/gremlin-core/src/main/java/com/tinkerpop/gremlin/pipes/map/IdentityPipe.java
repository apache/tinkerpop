package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.Holder;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityPipe<S> extends MapPipe<S, S> {

    public IdentityPipe(final Gremlin pipeline) {
        super(pipeline, Holder::get);
    }
}
