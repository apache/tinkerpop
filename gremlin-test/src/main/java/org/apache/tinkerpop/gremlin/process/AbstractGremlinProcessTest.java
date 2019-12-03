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
package org.apache.tinkerpop.gremlin.process;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

/**
 * Base test class for Gremlin Process tests.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinProcessTest extends AbstractGremlinTest {
    private static final Logger logger = LoggerFactory.getLogger(AbstractGremlinProcessTest.class);

    /**
     * Determines if a graph meets requirements for execution.  All gremlin process tests should check this
     * method as part of a call to {@code assumeTrue} to ensure that the test doesn't require the computer
     * feature or if it does require the computer feature then ensure that the graph being tested supports it.
     */
    protected boolean graphMeetsTestRequirements() {
        return !hasGraphComputerRequirement() || graph.features().graph().supportsComputer();
    }

    /**
     * Determines if this test suite has "computer" requirements.
     */
    protected boolean hasGraphComputerRequirement() {
        // do the negation of STANDARD as we expect a future type of REASONING that would infer COMPUTER support
        return !GraphManager.getTraversalEngineType().equals(TraversalEngine.Type.STANDARD);
    }

    @Before
    public void setupTest() {
        assumeTrue(graphMeetsTestRequirements());

        try {
            // ignore tests that aren't supported by a specific TraversalEngine
            final String testName = name.getMethodName();

            // tests that are parameterized have a square bracket with parameterized name appended to the actual
            // test method name. have to strip that off so that reflection can find it
            final String methodName = testName.contains("[") ? testName.substring(0, testName.indexOf('[')) : testName;
            final IgnoreEngine ignoreEngine = this.getClass().getMethod(methodName).getAnnotation(IgnoreEngine.class);
            if (ignoreEngine != null)
                assumeTrue(String.format("%s.%s is ignored for %s", this.getClass().getName(), testName, ignoreEngine.value()), !ignoreEngine.value().equals(GraphManager.getTraversalEngineType()));
        } catch (NoSuchMethodException nsme) {
            // some tests are parameterized
            throw new RuntimeException(String.format("Could not find test method %s in test case %s", name.getMethodName(), this.getClass().getName()));
        }
    }

    public static void checkSideEffects(final TraversalSideEffects sideEffects, final Object... keysClasses) {
        int counter = 0;
        for (int i = 0; i < keysClasses.length; i = i + 2) {
            counter++;
            final String key = (String) keysClasses[i];
            final Class clazz = (Class) keysClasses[i + 1];
            assertThat(sideEffects.keys().contains(key), is(true));
            assertThat(sideEffects.exists(key), is(true));
            assertThat(sideEffects.exists(UUID.randomUUID().toString()), is(false));
            assertEquals(clazz, sideEffects.get((String) keysClasses[i]).getClass());
        }
        assertEquals(sideEffects.keys().size(), counter);
        assertThat(sideEffects.keys().contains(UUID.randomUUID().toString()), is(false));
        assertEquals(StringFactory.traversalSideEffectsString(sideEffects), sideEffects.toString());
    }

    public static <T> void checkOrderedResults(final List<T> expectedResults, final Traversal<?, T> traversal) {
        final List<T> results = traversal.toList();
        assertFalse(traversal.hasNext());
        if (expectedResults.size() != results.size()) {
            logger.error("Expected results: " + expectedResults);
            logger.error("Actual results:   " + results);
            assertEquals("Checking result size", expectedResults.size(), results.size());
        }
        for (int i = 0; i < expectedResults.size(); i++) {
            assertEquals(expectedResults.get(i), results.get(i));
        }
    }

    public static <T> void checkResults(final List<T> expectedResults, final Traversal<?, T> traversal) {
        final List<T> results = traversal.toList();
        assertThat(traversal.hasNext(), is(false));
        if (expectedResults.size() != results.size()) {
            logger.error("Expected results: " + expectedResults);
            logger.error("Actual results:   " + results);
            assertEquals("Checking result size", expectedResults.size(), results.size());
        }

        for (T t : results) {
            if (t instanceof Map) {
                assertThat("Checking map result existence: " + t, expectedResults.stream().filter(e -> e instanceof Map).anyMatch(e -> internalCheckMap((Map) e, (Map) t)), is(true));
            } else if (t instanceof List) {
                assertThat("Checking list result existence: " + t, expectedResults.stream().filter(e -> e instanceof List).anyMatch(e -> internalCheckList((List) e, (List) t)), is(true));
            } else {
                assertThat("Checking result existence: " + t, expectedResults.contains(t), is(true));
            }
        }
        final Map<T, Long> expectedResultsCount = new HashMap<>();
        final Map<T, Long> resultsCount = new HashMap<>();
        expectedResults.forEach(t -> MapHelper.incr(expectedResultsCount, t, 1L));
        results.forEach(t -> MapHelper.incr(resultsCount, t, 1L));
        assertEquals("Checking indexing is equivalent", expectedResultsCount.size(), resultsCount.size());
        expectedResultsCount.forEach((k, v) -> assertEquals("Checking result group counts", v, resultsCount.get(k)));
    }

    public static <T> void checkResults(final Map<T, Long> expectedResults, final Traversal<?, T> traversal) {
        final List<T> list = new ArrayList<>();
        expectedResults.forEach((k, v) -> {
            for (int i = 0; i < v; i++) {
                list.add(k);
            }
        });
        checkResults(list, traversal);
    }

    public static <A, B> void checkMap(final Map<A, B> expectedMap, final Map<A, B> actualMap) {
        final List<Map.Entry<A, B>> actualList = actualMap.entrySet().stream().sorted(Comparator.comparing(a -> a.getKey().toString())).collect(Collectors.toList());
        final List<Map.Entry<A, B>> expectedList = expectedMap.entrySet().stream().sorted(Comparator.comparing(a -> a.getKey().toString())).collect(Collectors.toList());
        assertEquals(expectedList.size(), actualList.size());
        for (int i = 0; i < actualList.size(); i++) {
            assertEquals(expectedList.get(i).getKey(), actualList.get(i).getKey());
            assertEquals(expectedList.get(i).getValue(), actualList.get(i).getValue());
        }
    }

    private static <A> boolean internalCheckList(final List<A> expectedList, final List<A> actualList) {
        if (expectedList.size() != actualList.size()) {
            return false;
        }
        for (int i = 0; i < actualList.size(); i++) {
            if (!actualList.get(i).equals(expectedList.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static <A, B> boolean internalCheckMap(final Map<A, B> expectedMap, final Map<A, B> actualMap) {
        final List<Map.Entry<A, B>> actualList = actualMap.entrySet().stream().sorted(Comparator.comparing(a -> a.getKey().toString())).collect(Collectors.toList());
        final List<Map.Entry<A, B>> expectedList = expectedMap.entrySet().stream().sorted(Comparator.comparing(a -> a.getKey().toString())).collect(Collectors.toList());

        if (expectedList.size() != actualList.size()) {
            return false;
        }

        for (int i = 0; i < actualList.size(); i++) {
            if (!Objects.equals(actualList.get(i).getKey(), expectedList.get(i).getKey())) {
                return false;
            }
            if (!Objects.equals(actualList.get(i).getValue(), expectedList.get(i).getValue())) {
                return false;
            }
        }
        return true;
    }

    public <A, B> List<Map<A, B>> makeMapList(final int size, final Object... keyValues) {
        final List<Map<A, B>> mapList = new ArrayList<>();
        for (int i = 0; i < keyValues.length; i = i + (2 * size)) {
            final Map<A, B> map = new HashMap<>();
            for (int j = 0; j < (2 * size); j = j + 2) {
                map.put((A) keyValues[i + j], (B) keyValues[i + j + 1]);
            }
            mapList.add(map);
        }
        return mapList;
    }
}
