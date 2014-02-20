package com.tinkerpop.gremlin.process.steps.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.AnnotatedValue;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotationValueStep<E> extends MapStep<AnnotatedValue, E> {

    public String annotationKey;

    public AnnotationValueStep(final Traversal traversal, final String annotationKey) {
        super(traversal);
        this.annotationKey = annotationKey;
        this.setFunction(holder -> {
            final Optional<E> eOptional = holder.get().getAnnotation(annotationKey);
            return eOptional.isPresent() ? eOptional.get() : (E) NO_OBJECT;
        });
    }
}
