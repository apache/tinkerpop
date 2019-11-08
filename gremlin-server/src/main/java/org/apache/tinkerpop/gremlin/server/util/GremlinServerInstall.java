/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server.util;

import org.apache.tinkerpop.gremlin.groovy.util.Artifact;
import org.apache.tinkerpop.gremlin.groovy.util.DependencyGrabber;
import groovy.lang.GroovyClassLoader;

import java.io.File;

/**
 * Command line installer for plugins to Gremlin Server.
 *
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
                System.exit(1);
            }

        }
    }

    private static File getExtensionPath() {
        return new File(System.getProperty("user.dir"), "ext");
    }
}
