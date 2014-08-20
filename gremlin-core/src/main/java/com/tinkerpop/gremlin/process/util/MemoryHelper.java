package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryHelper {

    public static void validateMemory(final String key, final Object value) throws IllegalArgumentException {
        if (null == value)
            throw Traversal.SideEffects.Exceptions.sideEffectValueCanNotBeNull();
        if (null == key)
            throw Traversal.SideEffects.Exceptions.sideEffectKeyCanNotBeNull();
        if (key.isEmpty())
            throw Traversal.SideEffects.Exceptions.sideEffectKeyCanNotBeEmpty();
    }

    public static void legalMemoryKeyValueArray(final Object[] memoryKeyValues) {
        if (memoryKeyValues.length % 2 != 0)
            throw Traversal.SideEffects.Exceptions.sideEffectKeyValuesMustBeAMultipleOfTwo();
        for (int i = 0; i < memoryKeyValues.length; i = i + 2) {
            if (!(memoryKeyValues[i] instanceof String))
                throw Traversal.SideEffects.Exceptions.sideEffectKeyValuesMustHaveALegalKeyOnEvenIndices();
        }
    }

}
