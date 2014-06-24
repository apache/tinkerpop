package com.tinkerpop.gremlin.giraph.hdfs;

import com.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class KeyHelper {

    public static String makeDirectory(final String key) {
        return Graph.Key.isHidden(key) ? "~" + Graph.Key.unHide(key) : key;

    }
}
