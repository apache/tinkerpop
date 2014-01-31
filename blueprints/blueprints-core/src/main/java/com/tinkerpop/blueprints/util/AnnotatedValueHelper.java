package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.blueprints.Graph;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedValueHelper {

    public static void validateAnnotatedValue(final Object value) throws IllegalArgumentException {
        if (null == value)
            throw AnnotatedValue.Exceptions.annotatedValueCanNotBeNull();
    }

    public static void validateAnnotation(final String annotationKey, final Object annotationValue) throws IllegalArgumentException {
        if (null == annotationValue)
            throw AnnotatedValue.Exceptions.annotationValueCanNotBeNull();
        if (null == annotationKey)
            throw AnnotatedValue.Exceptions.annotationKeyCanNotBeNull();
        if (annotationKey.equals(AnnotatedValue.VALUE))
            throw AnnotatedValue.Exceptions.annotationKeyValueIsReserved();
        if (annotationKey.isEmpty())
            throw AnnotatedValue.Exceptions.annotationKeyCanNotBeEmpty();
    }

    public static void legalAnnotationKeyValueArray(final Object... annotationKeyValues) throws IllegalArgumentException {
        if (annotationKeyValues.length % 2 != 0)
            throw AnnotatedValue.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
        for (int i = 0; i < annotationKeyValues.length; i = i + 2) {
            if (!(annotationKeyValues[i] instanceof String))
                throw AnnotatedValue.Exceptions.providedKeyValuesMustHaveAStringOnEvenIndices();
            else
                validateAnnotation((String) annotationKeyValues[i], annotationKeyValues[i + 1]);
        }
    }

    public static void attachAnnotations(final AnnotatedValue annotatedValue, final Object... annotationKeyValues) {
        if (null == annotatedValue)
            throw Graph.Exceptions.argumentCanNotBeNull("annotatedValue");

        for (int i = 0; i < annotationKeyValues.length; i = i + 2) {
            if (!annotationKeyValues[i].equals(AnnotatedValue.VALUE))
                annotatedValue.setAnnotation((String) annotationKeyValues[i], annotationKeyValues[i + 1]);
        }
    }

    public static Map<String, Object> annotationMap(final AnnotatedValue<?> annotatedValue, final String... annotationKeys) {
        final Map<String, Object> values = new HashMap<>();
        if (null == annotationKeys || annotationKeys.length == 0) {
            annotatedValue.getAnnotationKeys().forEach(key -> values.put(key, annotatedValue.getAnnotation(key).get()));
        } else {
            for (final String key : annotationKeys) {
                if (key.equals(AnnotatedValue.VALUE))
                    values.put(AnnotatedValue.VALUE, annotatedValue.getValue());
                else
                    annotatedValue.getAnnotation(key).ifPresent(v -> values.put(key, v));
            }
        }
        return values;
    }
}
