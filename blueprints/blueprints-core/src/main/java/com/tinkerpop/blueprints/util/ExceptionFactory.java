package com.tinkerpop.blueprints.util;

import java.lang.reflect.Method;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ExceptionFactory {

    // how do you pass method references in Java8 ?
    public static UnsupportedOperationException generateUnsupportedOperation(final Consumer method) {
        return new UnsupportedOperationException("The method " + method.toString() + " is unsupported by this graph");
    }
}
