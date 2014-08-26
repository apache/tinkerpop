package com.tinkerpop.gremlin.process.graph.util;

import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Labels {

    public static List<String> of(final String... labels) {
        return Arrays.asList(labels);
    }
}
