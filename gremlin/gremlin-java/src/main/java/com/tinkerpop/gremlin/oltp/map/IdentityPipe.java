package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.MapPipe;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityPipe<E> extends MapPipe<E, E> {

    public IdentityPipe(final Pipeline pipeline) {
        super(pipeline, Holder::get);
    }
}
