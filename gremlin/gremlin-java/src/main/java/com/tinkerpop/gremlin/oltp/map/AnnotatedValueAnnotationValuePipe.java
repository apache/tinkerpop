package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.gremlin.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedValueAnnotationValuePipe<E> extends MapPipe<AnnotatedValue, E> {

    public AnnotatedValueAnnotationValuePipe(final Pipeline pipeline, final String annotationKey) {
        super(pipeline, holder -> (E) holder.get().getAnnotation(annotationKey).get());
    }
}
