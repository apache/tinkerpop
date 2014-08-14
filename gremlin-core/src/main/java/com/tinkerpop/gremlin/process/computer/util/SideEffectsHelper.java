package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.process.computer.SideEffects;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectsHelper {

    public static void validateValue(final Object value) throws IllegalArgumentException {
        if (null == value)
            throw SideEffects.Exceptions.sideEffectValueCanNotBeNull();
    }

    public static void validateKey(final String key) throws IllegalArgumentException {
        if (null == key)
            throw SideEffects.Exceptions.sideEffectKeyCanNotBeNull();
        if (key.isEmpty())
            throw SideEffects.Exceptions.sideEffectKeyCanNotBeEmpty();
    }
}
