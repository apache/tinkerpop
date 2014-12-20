package com.tinkerpop.gremlin.util;

import com.jcabi.manifests.Manifests;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class Gremlin {
    private static String version;
    static {
        version = Manifests.read("version");
    }

    public static String version() {
        return version;
    }
}
