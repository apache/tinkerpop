package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryHelper {

    public static void validateMemory(final String variable, final Object value) throws IllegalArgumentException {
        if (null == value)
            throw Graph.Memory.Exceptions.memoryValueCanNotBeNull();
        if (null == variable)
            throw Graph.Memory.Exceptions.memoryKeyCanNotBeNull();
        if (variable.isEmpty())
            throw Graph.Memory.Exceptions.memoryKeyCanNotBeEmpty();
    }
}
