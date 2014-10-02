package com.tinkerpop.gremlin.groovy.loaders

import com.tinkerpop.gremlin.structure.Graph

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */

class ObjectLoader {

    public void loadIt() {

        Map.metaClass.getAt = { final IntRange range ->
            final int size = delegate.size();
            int high = Math.min(size - 1, range.max());
            int low = Math.max(0, range.min());

            final Map tempMap = new LinkedHashMap();
            int c = 0;
            for (final Map.Entry entry : delegate.entrySet()) {
                if (c >= low && c <= high) {
                    tempMap.put(entry.getKey(), entry.getValue());
                }
                if (c > high) {
                    break;
                }
                c++;
            }
            return tempMap;
        }

        String.metaClass.negative = {
            return Graph.Key.isHidden((String) delegate) ? Graph.Key.unHide((String) delegate) : Graph.Key.hide((String) delegate);
        }
    }

    public static void load() {

        Map.metaClass.getAt = { final IntRange range ->
            final int size = delegate.size();
            int high = Math.min(size - 1, range.max());
            int low = Math.max(0, range.min());

            final Map tempMap = new LinkedHashMap();
            int c = 0;
            for (final Map.Entry entry : delegate.entrySet()) {
                if (c >= low && c <= high) {
                    tempMap.put(entry.getKey(), entry.getValue());
                }
                if (c > high) {
                    break;
                }
                c++;
            }
            return tempMap;
        }

        String.metaClass.negative = {
            return Graph.Key.isHidden((String) delegate) ? Graph.Key.unHide((String) delegate) : Graph.Key.hide((String) delegate);
        }
    }
}
