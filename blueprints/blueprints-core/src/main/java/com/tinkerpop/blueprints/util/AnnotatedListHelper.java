package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.AnnotatedList;
import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.blueprints.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedListHelper {

    public static void validateAnnotatedValue(final Object value) throws IllegalArgumentException {
        if (null == value)
            throw AnnotatedValue.Exceptions.annotatedValueCanNotBeNull();
    }

    public static void validateAnnotation(final String annotationKey, final Object annotationValue) throws IllegalArgumentException {
        if (null == annotationValue)
            throw AnnotatedValue.Exceptions.annotationValueCanNotBeNull();
        if (null == annotationKey)
            throw AnnotatedValue.Exceptions.annotationKeyCanNotBeNull();
        if (annotationKey.equals(AnnotatedValue.Key.VALUE))
            throw AnnotatedValue.Exceptions.annotationKeyValueIsReserved();
        if (annotationKey.isEmpty())
            throw AnnotatedValue.Exceptions.annotationKeyCanNotBeEmpty();
    }

    public static void legalAnnotationKeyValueArray(final Object... annotationKeyValues) throws IllegalArgumentException {
        if (annotationKeyValues.length % 2 != 0)
            throw AnnotatedList.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
        for (int i = 0; i < annotationKeyValues.length; i = i + 2) {
            if (!(annotationKeyValues[i] instanceof String))
                throw AnnotatedList.Exceptions.providedKeyValuesMustHaveAStringOnEvenIndices();
        }
    }

    public static void attachAnnotations(final AnnotatedValue annotatedValue, final Object... annotationKeyValues) {
        if (null == annotatedValue)
            throw Graph.Exceptions.argumentCanNotBeNull("annotatedValue");

        for (int i = 0; i < annotationKeyValues.length; i = i + 2) {
            if (!annotationKeyValues[i].equals(AnnotatedValue.Key.VALUE))
                annotatedValue.setAnnotation((String) annotationKeyValues[i], annotationKeyValues[i + 1]);
        }
    }
}
