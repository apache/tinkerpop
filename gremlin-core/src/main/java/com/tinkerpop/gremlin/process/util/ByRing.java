package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.structure.Element;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ByRing<A, B> extends FunctionRing<A, B> implements Cloneable {

    public ByRing(final Object... objectMappers) {
        this.functions = new Function[objectMappers.length];
        for (int i = 0; i < objectMappers.length; i++) {
            if (objectMappers[i] instanceof String) {
                final String elementPropertyKey = (String) objectMappers[i];
                this.functions[i] = element -> ((Element) element).value(elementPropertyKey);
            } else if (objectMappers[i] instanceof Function) {
                this.functions[i] = (Function) objectMappers[i];
            } else {
                throw new IllegalArgumentException("A by-ring only takes strings or functions: " + objectMappers[i]);
            }
        }
    }
}
