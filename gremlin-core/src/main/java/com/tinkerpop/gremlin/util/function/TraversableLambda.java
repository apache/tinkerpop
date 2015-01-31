package com.tinkerpop.gremlin.util.function;

import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversableLambda<S,E> extends Cloneable {

    public Traversal<S, E> getTraversal();

    public TraversableLambda clone() throws CloneNotSupportedException;

    public static <T> T tryAndClone(final Object object) throws CloneNotSupportedException {
        return (object instanceof TraversableLambda) ? (T) ((TraversableLambda) object).clone() : (T) object;

    }

}
