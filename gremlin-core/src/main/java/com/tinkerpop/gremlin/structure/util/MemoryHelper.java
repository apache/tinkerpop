package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryHelper {

    public static void validateMemory(final String annotationKey, final Object annotationValue) throws IllegalArgumentException {
        if (null == annotationValue)
            throw Graph.Memory.Exceptions.memoryValueCanNotBeNull();
        if (null == annotationKey)
            throw Graph.Memory.Exceptions.memoryKeyCanNotBeNull();
        if (annotationKey.isEmpty())
            throw Graph.Memory.Exceptions.memoryKeyCanNotBeEmpty();
    }
}
