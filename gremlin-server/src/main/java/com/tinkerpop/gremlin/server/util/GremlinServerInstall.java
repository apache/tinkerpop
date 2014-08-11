package com.tinkerpop.gremlin.server.util;

import com.tinkerpop.gremlin.groovy.plugin.Artifact;
import com.tinkerpop.gremlin.groovy.util.DependencyGrabber;
import groovy.lang.GroovyClassLoader;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServerInstall {
    private static GroovyClassLoader dummyClassLoader = new GroovyClassLoader();

    public static void main(final String[] arguments) {
        if (arguments.length != 3) {
            System.out.println("Usage: <group> <artifact> <version>");
        } else {
            try {
                final Artifact artifact = new Artifact(arguments[0], arguments[1], arguments[2]);
                final DependencyGrabber grabber = new DependencyGrabber(dummyClassLoader, getExtensionPath());
                grabber.copyDependenciesToPath(artifact);
            } catch (Exception iae) {
                System.out.println(String.format("Could not install the dependency: %s", iae.getMessage()));
                iae.printStackTrace();
            }

        }
    }

    private static String getExtensionPath() {
        final String fileSep = System.getProperty("file.separator");
        return System.getProperty("user.dir") + fileSep + "ext";
    }
}
