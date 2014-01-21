package com.tinkerpop.gremlin.oltp.filter;

import com.tinkerpop.gremlin.FilterPipe;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SimplePathPipe<S> extends FilterPipe<S> {

    public SimplePathPipe(final Pipeline pipeline) {
        super(pipeline, holder -> holder.getPath().isSimple());
    }
}
