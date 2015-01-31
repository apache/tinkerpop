package com.tinkerpop.gremlin.util;

import com.jcabi.manifests.Manifests;

import java.io.IOException;

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

    public static void main(final String[] arguments) throws IOException {
        System.out.println("gremlin " + version());
    }
}
