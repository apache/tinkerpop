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
package org.apache.tinkerpop.gremlin;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.URL;

import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TestHelper {
    public static void assertIsUtilityClass(final Class<?> utilityClass) throws Exception {
        final Constructor constructor = utilityClass.getDeclaredConstructor();
        assertTrue(Modifier.isFinal(utilityClass.getModifiers()));
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        constructor.newInstance();
    }

    /**
     * See {@code TestHelper} in gremlin-test for the official version.
     */
    public static File generateTempFile(final Class clazz, final String fileName, final String fileNameSuffix) throws IOException {
        final File path = makeTestDataPath(clazz, "temp");
        if (!path.exists()) path.mkdirs();
        return File.createTempFile(fileName, fileNameSuffix, path);
    }

    /**
     * See {@code TestHelper} in gremlin-test for the official version.
     */
    public static File makeTestDataPath(final Class clazz, final String childPath) {
        final String buildDirectory = System.getProperty("build.dir");
        final File root = null == buildDirectory ? new File(computePath(clazz)).getParentFile() : new File(buildDirectory);
        return new File(root, cleanPathSegment(childPath));
    }

    /**
     * See {@code TestHelper} in gremlin-test for the official version.
     */
    public static String cleanPathSegment(final String toClean) {
        final String cleaned = toClean.replaceAll("[.\\\\/:*?\"<>|\\[\\]\\(\\)]", "");
        if (cleaned.length() == 0)
            throw new IllegalStateException("Path segment " + toClean + " has not valid characters and is thus empty");
        return cleaned;
    }

    /**
     * See {@code TestHelper} in gremlin-test for the official version.
     */
    private static String computePath(final Class clazz) {
        final String clsUri = clazz.getName().replace('.', '/') + ".class";
        final URL url = clazz.getClassLoader().getResource(clsUri);
        final String clsPath = url.getPath();
        return clsPath.substring(0, clsPath.length() - clsUri.length());
    }
}
