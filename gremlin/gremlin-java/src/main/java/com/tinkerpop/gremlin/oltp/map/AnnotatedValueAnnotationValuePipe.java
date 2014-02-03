package com.tinkerpop.gremlin.oltp.map;

import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.gremlin.Pipeline;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedValueAnnotationValuePipe<E> extends MapPipe<AnnotatedValue, E> {

    public String annotationKey;

    public AnnotatedValueAnnotationValuePipe(final Pipeline pipeline, final String annotationKey) {
        super(pipeline);
        this.annotationKey = annotationKey;
        this.setFunction(holder -> {
            final Optional<E> eOptional = holder.get().getAnnotation(annotationKey);
            return eOptional.isPresent() ? eOptional.get() : (E) NO_OBJECT;
        });
    }
}
