package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityPipe<S> extends MapPipe<S, S> {

    public IdentityPipe(final Pipeline pipeline) {
        super(pipeline, Holder::get);
    }
}
