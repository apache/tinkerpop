package com.tinkerpop.gremlin.process.oltp.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.AnnotatedValue;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedValueAnnotationValuePipe<E> extends MapPipe<AnnotatedValue, E> {

    public String annotationKey;

    public AnnotatedValueAnnotationValuePipe(final Traversal pipeline, final String annotationKey) {
        super(pipeline);
        this.annotationKey = annotationKey;
        this.setFunction(holder -> {
            final Optional<E> eOptional = holder.get().getAnnotation(annotationKey);
            return eOptional.isPresent() ? eOptional.get() : (E) NO_OBJECT;
        });
    }
}
