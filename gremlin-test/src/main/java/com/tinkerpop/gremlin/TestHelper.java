package com.tinkerpop.gremlin;

import java.io.File;
import java.net.URL;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TestHelper {
    public static File computeTestDataRoot(final Class clazz, final String childPath) {
        final String clsUri = clazz.getName().replace('.', '/') + ".class";
        final URL url = clazz.getClassLoader().getResource(clsUri);
        final String clsPath = url.getPath();
        final File root = new File(clsPath.substring(0, clsPath.length() - clsUri.length()));
        return new File(root.getParentFile(), childPath);
    }
}
