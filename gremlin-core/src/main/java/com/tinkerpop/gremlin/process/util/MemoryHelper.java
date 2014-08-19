package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryHelper {

    public static void validateMemory(final String key, final Object value) throws IllegalArgumentException {
        if (null == value)
            throw Traversal.Memory.Exceptions.variableValueCanNotBeNull();
        if (null == key)
            throw Traversal.Memory.Exceptions.variableKeyCanNotBeNull();
        if (key.isEmpty())
            throw Traversal.Memory.Exceptions.variableKeyCanNotBeEmpty();
    }

    public static void legalMemoryKeyValueArray(final Object[] memoryKeyValues) {
        if (memoryKeyValues.length % 2 != 0)
            throw Traversal.Memory.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
        for (int i = 0; i < memoryKeyValues.length; i = i + 2) {
            if (!(memoryKeyValues[i] instanceof String))
                throw Traversal.Memory.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices();
        }
    }

}
