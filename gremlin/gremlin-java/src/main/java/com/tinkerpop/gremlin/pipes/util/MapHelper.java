package com.tinkerpop.gremlin.pipes.util;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapHelper {

    public static <T> void incr(final Map<T, Long> map, final T key, final Long value) {
        Long temp = map.get(key);
        temp = (null == temp) ? value : temp + value;
        map.put(key, temp);
    }
}
