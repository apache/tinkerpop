package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedValuePipe<E> extends MapPipe<AnnotatedValue<E>, E> {

    public AnnotatedValuePipe(final Pipeline pipeline) {
        super(pipeline, holder -> holder.get().getValue());
    }
}
