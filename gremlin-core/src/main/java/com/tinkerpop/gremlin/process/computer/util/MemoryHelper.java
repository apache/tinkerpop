package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.process.computer.Memory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MemoryHelper {

    public static void validateValue(final Object value) throws IllegalArgumentException {
        if (null == value)
            throw Memory.Exceptions.memoryValueCanNotBeNull();
    }

    public static void validateKey(final String key) throws IllegalArgumentException {
        if (null == key)
            throw Memory.Exceptions.memoryKeyCanNotBeNull();
        if (key.isEmpty())
            throw Memory.Exceptions.memoryKeyCanNotBeEmpty();
    }
}
