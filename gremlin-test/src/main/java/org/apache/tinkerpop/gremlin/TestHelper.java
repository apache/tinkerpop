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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

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

    public static void validateVertex(final Vertex originalVertex, final Vertex starVertex) {
        ////////////////  VALIDATE PROPERTIES
        final AtomicInteger originalPropertyCounter = new AtomicInteger(0);
        final AtomicInteger originalMetaPropertyCounter = new AtomicInteger(0);
        final AtomicInteger starPropertyCounter = new AtomicInteger(0);
        final AtomicInteger starMetaPropertyCounter = new AtomicInteger(0);

        originalVertex.properties().forEachRemaining(vertexProperty -> {
            originalPropertyCounter.incrementAndGet();
            starVertex.properties(vertexProperty.label()).forEachRemaining(starVertexProperty -> {
                if (starVertexProperty.equals(vertexProperty)) {
                    starPropertyCounter.incrementAndGet();
                    assertEquals(starVertexProperty.id(), vertexProperty.id());
                    assertEquals(starVertexProperty.label(), vertexProperty.label());
                    assertEquals(starVertexProperty.value(), vertexProperty.value());
                    assertEquals(starVertexProperty.key(), vertexProperty.key());
                    assertEquals(starVertexProperty.element(), vertexProperty.element());
                    //
                    vertexProperty.properties().forEachRemaining(p -> {
                        originalMetaPropertyCounter.incrementAndGet();
                        assertEquals(p.value(), starVertexProperty.property(p.key()).value());
                        assertEquals(p.key(), starVertexProperty.property(p.key()).key());
                        assertEquals(p.element(), starVertexProperty.property(p.key()).element());
                    });
                    starVertexProperty.properties().forEachRemaining(p -> starMetaPropertyCounter.incrementAndGet());
                }
            });
        });

        assertEquals(originalPropertyCounter.get(), starPropertyCounter.get());
        assertEquals(originalMetaPropertyCounter.get(), starMetaPropertyCounter.get());

        originalPropertyCounter.set(0);
        starPropertyCounter.set(0);
        originalMetaPropertyCounter.set(0);
        starMetaPropertyCounter.set(0);
        //
        starVertex.properties().forEachRemaining(starVertexProperty -> {
            starPropertyCounter.incrementAndGet();
            originalVertex.properties(starVertexProperty.label()).forEachRemaining(vertexProperty -> {
                if (starVertexProperty.equals(vertexProperty)) {
                    originalPropertyCounter.incrementAndGet();
                    assertEquals(vertexProperty.id(), starVertexProperty.id());
                    assertEquals(vertexProperty.label(), starVertexProperty.label());
                    assertEquals(vertexProperty.value(), starVertexProperty.value());
                    assertEquals(vertexProperty.key(), starVertexProperty.key());
                    assertEquals(vertexProperty.element(), starVertexProperty.element());
                    starVertexProperty.properties().forEachRemaining(p -> {
                        starMetaPropertyCounter.incrementAndGet();
                        assertEquals(p.value(), vertexProperty.property(p.key()).value());
                        assertEquals(p.key(), vertexProperty.property(p.key()).key());
                        assertEquals(p.element(), vertexProperty.property(p.key()).element());
                    });
                    vertexProperty.properties().forEachRemaining(p -> originalMetaPropertyCounter.incrementAndGet());
                }
            });
        });
        assertEquals(originalPropertyCounter.get(), starPropertyCounter.get());
        assertEquals(originalMetaPropertyCounter.get(), starMetaPropertyCounter.get());

        ////////////////  VALIDATE EDGES
        assertEquals(originalVertex, starVertex);
        assertEquals(starVertex, originalVertex);
        assertEquals(starVertex.id(), originalVertex.id());
        assertEquals(starVertex.label(), originalVertex.label());
        ///
        List<Edge> originalEdges = new ArrayList<>(IteratorUtils.set(originalVertex.edges(Direction.OUT)));
        List<Edge> starEdges = new ArrayList<>(IteratorUtils.set(starVertex.edges(Direction.OUT)));
        assertEquals(originalEdges.size(), starEdges.size());
        for (int i = 0; i < starEdges.size(); i++) {
            final Edge starEdge = starEdges.get(i);
            final Edge originalEdge = originalEdges.get(i);
            assertEquals(starEdge, originalEdge);
            assertEquals(starEdge.id(), originalEdge.id());
            assertEquals(starEdge.label(), originalEdge.label());
            assertEquals(starEdge.inVertex(), originalEdge.inVertex());
            assertEquals(starEdge.outVertex(), originalEdge.outVertex());
            originalEdge.properties().forEachRemaining(p -> assertEquals(p, starEdge.property(p.key())));
            starEdge.properties().forEachRemaining(p -> assertEquals(p, originalEdge.property(p.key())));
        }

        originalEdges = new ArrayList<>(IteratorUtils.set(originalVertex.edges(Direction.IN)));
        starEdges = new ArrayList<>(IteratorUtils.set(starVertex.edges(Direction.IN)));
        assertEquals(originalEdges.size(), starEdges.size());
        for (int i = 0; i < starEdges.size(); i++) {
            final Edge starEdge = starEdges.get(i);
            final Edge originalEdge = originalEdges.get(i);
            assertEquals(starEdge, originalEdge);
            assertEquals(starEdge.id(), originalEdge.id());
            assertEquals(starEdge.label(), originalEdge.label());
            assertEquals(starEdge.inVertex(), originalEdge.inVertex());
            assertEquals(starEdge.outVertex(), originalEdge.outVertex());
            originalEdge.properties().forEachRemaining(p -> assertEquals(p, starEdge.property(p.key())));
            starEdge.properties().forEachRemaining(p -> assertEquals(p, originalEdge.property(p.key())));
        }
    }
}
