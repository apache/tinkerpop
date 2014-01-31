package com.tinkerpop.gremlin.oltp.filter;

import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.query.util.HasContainer;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.util.GremlinHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IntervalPipe<S> extends FilterPipe<S> {

    public HasContainer startContainer;
    public HasContainer endContainer;

    public IntervalPipe(final Pipeline pipeline, final HasContainer startContainer, final HasContainer endContainer) {
        super(pipeline);
        this.startContainer = startContainer;
        this.endContainer = endContainer;
        this.setPredicate(holder -> {
            final S s = holder.get();
            if (s instanceof Element)
                return startContainer.test((Element) s) && endContainer.test((Element) s);
            else if (s instanceof AnnotatedValue)
                return startContainer.test((AnnotatedValue) s) && endContainer.test((AnnotatedValue) s);
            else
                throw new IllegalArgumentException("The provided class can not be check with has(): " + s.getClass());
        });
    }

    public String toString() {
        return GremlinHelper.makePipeString(this, this.startContainer, this.endContainer);
    }
}
