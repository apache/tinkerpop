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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Comparators;
import org.apache.tinkerpop.gremlin.util.CoreTestHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for test development.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 *
 * NOTE: This class duplicates the TestHelper class from gremlin-core. One of them must be removed.
 */
public final class TestHelper extends CoreTestHelper {
    private static final Logger logger = LoggerFactory.getLogger(TestHelper.class);

    public static final Random RANDOM;

    static {
        final long seed = Long.parseLong(System.getProperty("testSeed", String.valueOf(System.currentTimeMillis())));
        logger.info("*** THE RANDOM TEST SEED IS {} ***", seed);
        RANDOM = new Random(seed);
    }

    private TestHelper() {
    }

    public static void assertIsUtilityClass(final Class<?> utilityClass) throws Exception {
        final Constructor constructor = utilityClass.getDeclaredConstructor();

        assertTrue(Modifier.isFinal(utilityClass.getModifiers()));
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        constructor.newInstance();
    }
    
    public static String convertToRelative(final Class clazz, final File f) {
        final File root = getRootOfBuildDirectory(clazz).getParentFile();
        return root.toURI().relativize(f.toURI()).toString();
    }


    /**
     * Used at the start of a test to make it one that should only be executed when the {@code assertNonDeterministic}
     * system property is set to {@code true}. Tests that call this method are ones that may sometimes fail in certain
     * environments or behave in other random ways. Usually such tests should be removed or re-worked, but there are
     * situations where that may not be possible as there is no other good way to test the feature. In these cases, the
     * tests won't fail a standard build. For this benefit, the downside is that the feature isn't tested as often as
     * it would otherwise, since the {@code assertNonDeterministic} option is not used often and definitely not in
     * automated builds like Travis.
     */
    public static void assumeNonDeterministic() {
        assumeThat("Set the 'assertNonDeterministic' property to true to execute this test",
                System.getProperty("assertNonDeterministic"), is("true"));
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

    public static void createRandomGraph(final Graph graph, final int numberOfVertices, final int maxNumberOfEdgesPerVertex) {
        final Random random = new Random();
        for (int i = 0; i < numberOfVertices; i++) {
            graph.addVertex(T.id, i);
        }
        graph.vertices().forEachRemaining(vertex -> {
            for (int i = 0; i < random.nextInt(maxNumberOfEdgesPerVertex); i++) {
                final Vertex other = graph.vertices(random.nextInt(numberOfVertices)).next();
                vertex.addEdge("link", other);
            }
        });
    }
}
