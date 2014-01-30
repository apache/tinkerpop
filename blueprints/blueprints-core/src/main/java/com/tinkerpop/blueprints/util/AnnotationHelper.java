package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.AnnotatedList;
import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.blueprints.Annotations;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotationHelper {

    public static void validatedAnnotatedValue(final Object value) throws IllegalArgumentException {
        if (null == value)
            throw Annotations.Exceptions.annotationValueCanNotBeNull();
    }

    public static void validateAnnotation(final String key, final Object value) throws IllegalArgumentException {
        if (null == value)
            throw Annotations.Exceptions.annotationValueCanNotBeNull();
        if (null == key)
            throw Annotations.Exceptions.annotationKeyCanNotBeNull();
        if (key.equals(AnnotatedValue.Key.VALUE))
            throw Annotations.Exceptions.annotationKeyValueIsReserved();
        if (key.isEmpty())
            throw Annotations.Exceptions.annotationKeyCanNotBeEmpty();
    }

    public static void legalKeyValues(final Object... keyValues) throws IllegalArgumentException {
        if (keyValues.length % 2 != 0)
            throw AnnotatedList.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (!(keyValues[i] instanceof String))
                throw AnnotatedList.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices();
            else if (keyValues[i].equals(AnnotatedValue.Key.VALUE))
                throw AnnotatedValue.Exceptions.annotationKeyValueIsReserved();

        }
    }
}
