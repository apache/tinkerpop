package com.tinkerpop.gremlin.pipes.util;

import java.util.ArrayList;
import java.util.List;
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

    public static <T, U> void incr(final Map<T, List<U>> map, final T key, final U value) {
        List<U> temp = map.get(key);
        if (null == temp)
            temp = new ArrayList<>();
        temp.add(value);
        map.put(key, temp);
    }
}
