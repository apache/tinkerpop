package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphHelper {

    public static void validateAnnotation(final String annotationKey, final Object annotationValue) throws IllegalArgumentException {
        if (null == annotationValue)
            throw Graph.Annotations.Exceptions.graphAnnotationValueCanNotBeNull();
        if (null == annotationKey)
            throw Graph.Annotations.Exceptions.graphAnnotationKeyCanNotBeNull();
        if (annotationKey.isEmpty())
            throw Graph.Annotations.Exceptions.graphAnnotationKeyCanNotBeEmpty();
    }
}
