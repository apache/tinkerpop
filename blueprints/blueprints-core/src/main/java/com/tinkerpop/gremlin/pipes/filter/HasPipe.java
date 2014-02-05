package com.tinkerpop.gremlin.pipes.filter;

import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.query.util.HasContainer;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.util.GremlinHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HasPipe<S> extends FilterPipe<S> {

    public HasContainer hasContainer;

    public HasPipe(final Gremlin pipeline, final HasContainer hasContainer) {
        super(pipeline);
        this.hasContainer = hasContainer;
        this.setPredicate(holder -> {
            final S s = holder.get();
            if (s instanceof Element)
                return hasContainer.test((Element) s);
            else if (s instanceof AnnotatedValue)
                return hasContainer.test((AnnotatedValue) s);
            else
                throw new IllegalArgumentException("The provided class can not be check with has(): " + s.getClass());

        });
    }

    public String toString() {
        return GremlinHelper.makePipeString(this, this.hasContainer);
    }
}
