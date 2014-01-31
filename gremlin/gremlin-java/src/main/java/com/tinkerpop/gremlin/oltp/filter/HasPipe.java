package com.tinkerpop.gremlin.oltp.filter;

import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.query.util.HasContainer;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.util.GremlinHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasPipe<S> extends FilterPipe<S> {

    public HasContainer hasContainer;

    public HasPipe(final Pipeline pipeline, final HasContainer hasContainer) {
        super(pipeline);
        this.hasContainer = hasContainer;
        this.setPredicate(holder -> {
            final S temp = holder.get();
            if (temp instanceof Element)
                return hasContainer.test((Element) temp);
            else if (temp instanceof AnnotatedValue)
                return hasContainer.test((AnnotatedValue) temp);
            else
                throw new IllegalArgumentException("The provided class can not be check with has(): " + temp.getClass());

        });
    }

    public String toString() {
        return GremlinHelper.makePipeString(this, this.hasContainer);
    }
}
