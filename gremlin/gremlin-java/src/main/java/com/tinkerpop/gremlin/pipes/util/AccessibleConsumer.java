package com.tinkerpop.gremlin.pipes.util;

import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface AccessibleConsumer<T, R> extends Consumer<T> {

    public R getAccessible();
}
