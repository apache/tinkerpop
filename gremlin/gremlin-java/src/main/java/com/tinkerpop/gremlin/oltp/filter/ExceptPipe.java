package com.tinkerpop.gremlin.oltp.filter;

import com.tinkerpop.gremlin.Pipeline;

import java.util.Collection;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExceptPipe<S> extends FilterPipe<S> {

    public ExceptPipe(final Pipeline pipeline, final String variable) {
        super(pipeline);
        final Object temp = this.pipeline.get(variable);
        if (temp instanceof Collection)
            this.setPredicate(holder -> !((Collection) temp).contains(holder.get()));
        else
            this.setPredicate(holder -> !temp.equals(holder.get()));
    }

    public ExceptPipe(final Pipeline pipeline, final Collection<S> exceptionCollection) {
        super(pipeline);
        this.setPredicate(holder -> !exceptionCollection.contains(holder.get()));
    }

    public ExceptPipe(final Pipeline pipeline, final S exceptionObject) {
        super(pipeline);
        this.setPredicate(holder -> !exceptionObject.equals(holder.get()));
    }
}
