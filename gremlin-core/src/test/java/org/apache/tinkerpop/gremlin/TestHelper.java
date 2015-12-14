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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TestHelper {

    private static final String SEP = File.separator;
    public static final String TEST_DATA_RELATIVE_DIR = "test-case-data";

    private TestHelper() {}

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
    public static File makeTestDataPath(final Class clazz, final String... childPath) {
        final File root = getRootOfBuildDirectory(clazz);
        final List<String> cleanedPaths = Stream.of(childPath).map(TestHelper::cleanPathSegment).collect(Collectors.toList());

        // use the class name in the directory structure
        cleanedPaths.add(0, cleanPathSegment(clazz.getSimpleName()));

        final File f = new File(root, TEST_DATA_RELATIVE_DIR + SEP + String.join(SEP, cleanedPaths));
        if (!f.exists()) f.mkdirs();
        return f;
    }

    /**
     * See {@code TestHelper} in gremlin-test for the official version.
     */
    public static String makeTestDataDirectory(final Class clazz, final String... childPath) {
        return makeTestDataPath(clazz, childPath).getAbsolutePath() + SEP;
    }

    /**
     * See {@code TestHelper} in gremlin-test for the official version.
     */
    public static File getRootOfBuildDirectory(final Class clazz) {
        // build.dir gets sets during runs of tests with maven via the surefire configuration in the pom.xml
        // if that is not set as an environment variable, then the path is computed based on the location of the
        // requested class.  the computed version at least as far as intellij is concerned comes drops it into
        // /target/test-classes.  the build.dir had to be added because maven doesn't seem to like a computed path
        // as it likes to find that path in the .m2 directory and other weird places......
        final String buildDirectory = System.getProperty("build.dir");
        final File root = null == buildDirectory ? new File(computePath(clazz)).getParentFile() : new File(buildDirectory);
        if (!root.exists()) root.mkdirs();
        return root;
    }

    private static String computePath(final Class clazz) {
        final String clsUri = clazz.getName().replace('.', SEP.charAt(0)) + ".class";
        final URL url = clazz.getClassLoader().getResource(clsUri);
        final String clsPath = url.getPath();
        return clsPath.substring(0, clsPath.length() - clsUri.length());
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
    public static File generateTempFileFromResource(final Class resourceClass, final String resourceName, final String extension) throws IOException {
        final File temp = makeTestDataPath(resourceClass, "resources");
        if (!temp.exists()) temp.mkdirs();
        final File tempFile = new File(temp, resourceName + extension);
        final FileOutputStream outputStream = new FileOutputStream(tempFile);
        int data;
        final InputStream inputStream = resourceClass.getResourceAsStream(resourceName);
        while ((data = inputStream.read()) != -1) {
            outputStream.write(data);
        }
        outputStream.close();
        inputStream.close();
        return tempFile;
    }

    /**
     * See {@code TestHelper} in gremlin-test for the official version.
     */
    public static String convertPackageToResourcePath(final Class clazz) {
        final String packageName = clazz.getPackage().getName();
        return String.format("/%s/", packageName.replaceAll("\\.", "\\/"));
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
}
