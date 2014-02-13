package com.tinkerpop.gremlin.process.steps.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapHelper {

    public static <T> void incr(final Map<T, Long> map, final T key, final Long value) {
        map.put(key, value + map.getOrDefault(key, 0l));
    }

    public static <T, U> void incr(final Map<T, List<U>> map, final T key, final U value) {
        final List<U> temp = map.getOrDefault(key, new ArrayList<>());
        temp.add(value);
        map.put(key, temp);
    }
}
