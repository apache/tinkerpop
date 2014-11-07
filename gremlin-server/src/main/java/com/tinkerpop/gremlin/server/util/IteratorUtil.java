package com.tinkerpop.gremlin.server.util;

import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.collections.iterators.ArrayIterator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IteratorUtil {

    public static Iterator convertToIterator(final Object o) {
        final Iterator itty;
        if (o instanceof Iterable)
            itty = ((Iterable) o).iterator();
        else if (o instanceof Iterator)
            itty = (Iterator) o;
        else if (o instanceof Object[])
            itty = new ArrayIterator(o);
        else if (o instanceof Stream)
            itty = ((Stream) o).iterator();
        else if (o instanceof Map)
            itty = ((Map) o).entrySet().iterator();
        else if (o instanceof Throwable)
            itty = new SingleIterator<Object>(((Throwable) o).getMessage());
        else
            itty = new SingleIterator<>(o);
        return itty;
    }

    public static List convertToList(final Object o) {
        return (List) StreamFactory.stream(convertToIterator(o)).collect(Collectors.toList());
    }
}
