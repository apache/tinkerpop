package com.tinkerpop.gremlin.process.oltp.util;

import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class As {

    public static List<String> of(final String... asLabels) {
        return Arrays.asList(asLabels);
    }
}
