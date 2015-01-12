package com.tinkerpop.gremlin.process.util;

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

    public static <T> void incr(final Map<T, Double> map, final T key, final Double value) {
        map.put(key, value + map.getOrDefault(key, 0.0d));
    }

    public static <T, U> void incr(final Map<T, List<U>> map, final T key, final U value) {
        map.compute(key, (k, v) -> {
            if (null == v) v = new ArrayList<>();
            v.add(value);
            return v;
        });
    }
}
