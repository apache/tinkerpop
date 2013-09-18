package com.tinkerpop.gremlin;

import java.util.Iterator;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinPipeline<S, E>  {

    private final Iterator iterator;

    public GremlinPipeline(final Iterable iterable) {
        this.iterator = iterable.iterator();
    }



}
