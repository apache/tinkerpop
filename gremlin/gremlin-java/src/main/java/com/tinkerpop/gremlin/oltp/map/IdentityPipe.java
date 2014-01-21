package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.gremlin.AbstractPipe;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityPipe<S> extends AbstractPipe<S, S> {

    public IdentityPipe(final Pipeline pipeline) {
        super(pipeline);
    }

    protected Holder<S> processNextStart() {
        return this.starts.next();
    }
}
