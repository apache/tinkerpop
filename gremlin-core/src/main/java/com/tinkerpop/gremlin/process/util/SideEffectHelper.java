package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectHelper {

    public static void validateSideEffect(final String key, final Object value) throws IllegalArgumentException {
        if (null == value)
            throw Traversal.SideEffects.Exceptions.sideEffectValueCanNotBeNull();
        if (null == key)
            throw Traversal.SideEffects.Exceptions.sideEffectKeyCanNotBeNull();
        if (key.isEmpty())
            throw Traversal.SideEffects.Exceptions.sideEffectKeyCanNotBeEmpty();
    }
}
