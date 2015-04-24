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
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TestHelper {

    /**
     * Creates a {@link File} reference that points to a directory relative to the supplied class in the
     * {@code /target} directory.
     */
    public static File makeTestDataPath(final Class clazz, final String childPath) {
        // build.dir gets sets during runs of tests with maven via the surefire configuration in the pom.xml
        // if that is not set as an environment variable, then the path is computed based on the location of the
        // requested class.  the computed version at least as far as intellij is concerned comes drops it into
        // /target/test-classes.  the build.dir had to be added because maven doesn't seem to like a computed path
        // as it likes to find that path in the .m2 directory and other weird places......
        final String buildDirectory = System.getProperty("build.dir");
        final File root = null == buildDirectory ? new File(computePath(clazz)).getParentFile() : new File(buildDirectory);
        return new File(root, cleanPathSegment(childPath));
    }

    private static String computePath(final Class clazz) {
        final String clsUri = clazz.getName().replace('.', '/') + ".class";
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
            Iterator<Edge> originalEdges = IteratorUtils.set(originalVertex.edges(Direction.OUT)).iterator();
            Iterator<Edge> otherEdges = IteratorUtils.set(otherVertex.edges(Direction.OUT)).iterator();
            while (originalEdges.hasNext()) {
                validateEdgeEquality(originalEdges.next(), otherEdges.next());
            }
            assertFalse(otherEdges.hasNext());

            originalEdges = IteratorUtils.set(originalVertex.edges(Direction.IN)).iterator();
            otherEdges = IteratorUtils.set(otherVertex.edges(Direction.IN)).iterator();
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
            assertEquals(originalVertexProperty.keys().size(), otherVertexProperty.keys().size());
            for (final String key : originalVertexProperty.keys()) {
                validatePropertyEquality(originalVertexProperty.property(key), otherVertexProperty.property(key));
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
