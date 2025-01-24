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
package org.apache.tinkerpop.gremlin.util;

import java.io.IOException;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class Gremlin {
    private final static String gremlinVersion = "4.0.0-SNAPSHOT"; // DO NOT MODIFY - Configured automatically by Maven Replacer Plugin

    private Gremlin() {
    }

    /**
     * Get the current version of tinkerpop. Will return "VersionNotFound" if there are any issues finding
     * the version. This typically would be the result of the version being missing from the manifest file.
     */
    public static String version() {
        return gremlinVersion;
    }

    /**
     * Gets the current major version of tinkerpop.
     */
    public static String majorVersion() { return version().split("\\.")[0]; }

    public static void main(final String[] arguments) throws IOException {
        System.out.println("gremlin " + version());
    }
}
