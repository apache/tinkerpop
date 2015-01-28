package com.tinkerpop.gremlin.util.tools;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface FinalGet<A> {

    public A getFinal();

    public static <A> A tryFinalGet(final Object object) {
        return object instanceof FinalGet ? ((FinalGet<A>) object).getFinal() : (A) object;
    }
}
