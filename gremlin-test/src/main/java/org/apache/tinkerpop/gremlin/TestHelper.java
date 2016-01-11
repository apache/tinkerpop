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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Comparators;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Utility methods for test development.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TestHelper {

    private static final String SEP = File.separator;
    public static final String TEST_DATA_RELATIVE_DIR = "test-case-data";

    private TestHelper() {}

    /**
     * Creates a {@link File} reference that points to a directory relative to the supplied class in the
     * {@code /target} directory. Each {@code childPath} passed introduces a new sub-directory and all are placed
     * below the {@link #TEST_DATA_RELATIVE_DIR}.  For example, calling this method with "a", "b", and "c" as the
     * {@code childPath} arguments would yield a relative directory like: {@code test-case-data/clazz/a/b/c}. It is
     * a good idea to use the test class for the {@code clazz} argument so that it's easy to find the data if
     * necessary after test execution.
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

    public static String convertToRelative(final Class clazz, final File f) {
        final File root = TestHelper.getRootOfBuildDirectory(clazz).getParentFile().getAbsoluteFile();
        return root.toURI().relativize(f.getAbsoluteFile().toURI()).toString();
    }

    /**
     * Internally calls {@link #makeTestDataPath(Class, String...)} but returns the path as a string with the system
     * separator appended to the end.
     */
    public static String makeTestDataDirectory(final Class clazz, final String... childPath) {
        return makeTestDataPath(clazz, childPath).getAbsolutePath() + SEP;
    }

    /**
     * Gets and/or creates the root of the test data directory.  This  method is here as a convenience and should not
     * be used to store test data.  Use {@link #makeTestDataPath(Class, String...)} instead.
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
     * Creates a {@link File} reference in the path returned from {@link TestHelper#makeTestDataPath} in a subdirectory
     * called {@code temp}.
     */
    public static File generateTempFile(final Class clazz, final String fileName, final String fileNameSuffix) throws IOException {
        final File path = makeTestDataPath(clazz, "temp");
        if (!path.exists()) path.mkdirs();
        return File.createTempFile(fileName, fileNameSuffix, path);
    }

    /**
     * Copies a file stored as part of a resource to the file system in the path returned from
     * {@link TestHelper#makeTestDataPath} in a subdirectory called {@code temp/resources}.
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
     * Takes a class and converts its package name to a path that can be used to access a resource in that package.
     */
    public static String convertPackageToResourcePath(final Class clazz) {
        final String packageName = clazz.getPackage().getName();
        return String.format("/%s/", packageName.replaceAll("\\.", "\\/"));
    }

    /**
     * Removes characters that aren't acceptable in a file path (mostly for windows).
     */
    public static String cleanPathSegment(final String toClean) {
        final String cleaned = toClean.replaceAll("[.\\\\/:*?\"<>|\\[\\]\\(\\)]", "");
        if (cleaned.length() == 0)
            throw new IllegalStateException("Path segment " + toClean + " has not valid characters and is thus empty");
        return cleaned;
    }

    ///////////////

    public static void validateVertexEquality(final Vertex originalVertex, final Vertex otherVertex, boolean testEdges) {
        assertEquals(originalVertex, otherVertex);
        assertEquals(otherVertex, originalVertex);
        assertEquals(originalVertex.id(), otherVertex.id());
        assertEquals(originalVertex.label(), otherVertex.label());
        assertEquals(originalVertex.keys().size(), otherVertex.keys().size());
        for (final String key : originalVertex.keys()) {
            final List<VertexProperty<Object>> originalVertexProperties = IteratorUtils.list(originalVertex.properties(key));
            final List<VertexProperty<Object>> otherVertexProperties = IteratorUtils.list(otherVertex.properties(key));
            assertEquals(originalVertexProperties.size(), otherVertexProperties.size());
            for (VertexProperty<Object> originalVertexProperty : originalVertexProperties) {
                final VertexProperty<Object> otherVertexProperty = otherVertexProperties.parallelStream().filter(vp -> vp.equals(originalVertexProperty)).findAny().get();
                validateVertexPropertyEquality(originalVertexProperty, otherVertexProperty);
            }
        }
        if (testEdges) {
            Iterator<Edge> originalEdges = IteratorUtils.list(originalVertex.edges(Direction.OUT), Comparators.ELEMENT_COMPARATOR).iterator();
            Iterator<Edge> otherEdges = IteratorUtils.list(otherVertex.edges(Direction.OUT), Comparators.ELEMENT_COMPARATOR).iterator();
            while (originalEdges.hasNext()) {
                validateEdgeEquality(originalEdges.next(), otherEdges.next());
            }
            assertFalse(otherEdges.hasNext());

            originalEdges = IteratorUtils.list(originalVertex.edges(Direction.IN), Comparators.ELEMENT_COMPARATOR).iterator();
            otherEdges = IteratorUtils.list(otherVertex.edges(Direction.IN), Comparators.ELEMENT_COMPARATOR).iterator();
            while (originalEdges.hasNext()) {
                validateEdgeEquality(originalEdges.next(), otherEdges.next());
            }
            assertFalse(otherEdges.hasNext());
        }

    }

    public static void validateVertexPropertyEquality(final VertexProperty originalVertexProperty, final VertexProperty otherVertexProperty) {
        assertEquals(originalVertexProperty, otherVertexProperty);
        assertEquals(otherVertexProperty, originalVertexProperty);

        if (originalVertexProperty.isPresent()) {
            assertEquals(originalVertexProperty.key(), otherVertexProperty.key());
            assertEquals(originalVertexProperty.value(), otherVertexProperty.value());
            assertEquals(originalVertexProperty.element(), otherVertexProperty.element());

            final boolean originalSupportsMetaProperties = originalVertexProperty.graph().features().vertex().supportsMetaProperties();
            final boolean otherSupportsMetaProperties = otherVertexProperty.graph().features().vertex().supportsMetaProperties();

            // if one supports and the other doesn't then neither should have meta properties.
            if (originalSupportsMetaProperties && !otherSupportsMetaProperties)
                assertEquals(0, originalVertexProperty.keys().size());
            else if (!originalSupportsMetaProperties && otherSupportsMetaProperties)
                assertEquals(0, otherVertexProperty.keys().size());
            else {
                // both support it, so assert in full
                assertEquals(originalVertexProperty.keys().size(), otherVertexProperty.keys().size());
                for (final String key : originalVertexProperty.keys()) {
                    validatePropertyEquality(originalVertexProperty.property(key), otherVertexProperty.property(key));
                }
            }
        }
    }

    public static void validatePropertyEquality(final Property originalProperty, final Property otherProperty) {
        assertEquals(originalProperty, otherProperty);
        assertEquals(otherProperty, originalProperty);
        if (originalProperty.isPresent()) {
            assertEquals(originalProperty.key(), otherProperty.key());
            assertEquals(originalProperty.value(), otherProperty.value());
            assertEquals(originalProperty.element(), otherProperty.element());
        }
    }

    public static void validateEdgeEquality(final Edge originalEdge, final Edge otherEdge) {
        assertEquals(originalEdge, otherEdge);
        assertEquals(otherEdge, originalEdge);
        assertEquals(originalEdge.id(), otherEdge.id());
        assertEquals(originalEdge.label(), otherEdge.label());
        assertEquals(originalEdge.inVertex(), otherEdge.inVertex());
        assertEquals(originalEdge.outVertex(), otherEdge.outVertex());
        assertEquals(originalEdge.keys().size(), otherEdge.keys().size());
        for (final String key : originalEdge.keys()) {
            validatePropertyEquality(originalEdge.property(key), otherEdge.property(key));
        }
    }

    public static void validateEquality(final Object original, final Object other) {
        if (original instanceof Vertex)
            validateVertexEquality((Vertex) original, (Vertex) other, true);
        else if (original instanceof VertexProperty)
            validateVertexPropertyEquality((VertexProperty) original, (VertexProperty) other);
        else if (original instanceof Edge)
            validateEdgeEquality((Edge) original, (Edge) other);
        else if (original instanceof Property)
            validatePropertyEquality((Property) original, (Property) other);
        else
            throw new IllegalArgumentException("The provided object must be a graph object: " + original.getClass().getCanonicalName());
    }
}
