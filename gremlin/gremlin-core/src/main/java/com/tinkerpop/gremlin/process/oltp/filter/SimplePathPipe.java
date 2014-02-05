package com.tinkerpop.gremlin.process.oltp.filter;


import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SimplePathPipe<S> extends FilterPipe<S> {

    public SimplePathPipe(final Traversal pipeline) {
        super(pipeline, holder -> holder.getPath().isSimple());
    }
}
