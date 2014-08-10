package com.tinkerpop.gremlin.util.function;

import java.io.Serializable;
import java.util.function.BiConsumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface SBiConsumer<A, B> extends BiConsumer<A, B>, Serializable {
}
