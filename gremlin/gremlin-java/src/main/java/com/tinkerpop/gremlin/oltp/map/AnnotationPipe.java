package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotationPipe<E> extends MapPipe<AnnotatedValue, E> {

    public AnnotationPipe(final Pipeline pipeline, final String annotationKey) {
        super(pipeline, holder -> (E) holder.get().getAnnotation(annotationKey).get());
    }
}
