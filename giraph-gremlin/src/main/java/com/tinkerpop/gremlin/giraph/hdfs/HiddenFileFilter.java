package com.tinkerpop.gremlin.giraph.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HiddenFileFilter implements PathFilter {

    private static final HiddenFileFilter INSTANCE = new HiddenFileFilter();

    private HiddenFileFilter() {

    }

    public boolean accept(final Path path) {
        final String name = path.getName();
        return !name.startsWith("_") && !name.startsWith(".");
    }

    public static HiddenFileFilter instance() {
        return INSTANCE;
    }
}