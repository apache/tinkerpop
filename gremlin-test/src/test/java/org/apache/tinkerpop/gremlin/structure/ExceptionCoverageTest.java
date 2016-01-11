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
package org.apache.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.ExceptionCoverage;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest;
import org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

/**
 * A set of tests that ensure that the {@link ExceptionCoverageTest} covers all defined TinkerPop exceptions.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Pieter Martin
 */
public class ExceptionCoverageTest {
    private static final Logger logger = LoggerFactory.getLogger(ExceptionCoverageTest.class);

    @Test
    public void shouldCoverAllExceptionsInTests() {

        // these are the classes that have Exceptions that need to be checked.
        final Class[] exceptionDefinitionClasses = {
                Edge.Exceptions.class,
                Element.Exceptions.class,
                Graph.Exceptions.class,
                GraphComputer.Exceptions.class,
                Graph.Variables.Exceptions.class,
                Property.Exceptions.class,
                Transaction.Exceptions.class,
                Vertex.Exceptions.class,
                VertexProperty.Exceptions.class
        };

        // this is a list of exceptions that are "excused" from the coverage tests.  in most cases there should only
        // be exceptions ignored if they are "base" exception methods that are used to compose other exceptions,
        // like the first set listed (and labelled as such) below in the list assignments.
        final Set<String> ignore = new HashSet<String>() {{
            // this is a general exception to be used as needed. it is not explicitly tested:
            add("org.apache.tinkerpop.gremlin.structure.Graph$Exceptions#argumentCanNotBeNull");
            add("org.apache.tinkerpop.gremlin.structure.Graph$Exceptions#traversalEngineNotSupported");

            // deprecated exceptions
            add("org.apache.tinkerpop.gremlin.structure.Element$Exceptions#elementAlreadyRemoved"); // as of 3.1.0

            // need to write consistency tests for the following items still...........
            add("org.apache.tinkerpop.gremlin.process.computer.GraphComputer$Exceptions#supportsDirectObjects");
        }};

        // register test classes here that contain @ExceptionCoverage annotations
        final List<Class<?>> testClassesThatContainConsistencyChecks = new ArrayList<>();
        testClassesThatContainConsistencyChecks.addAll(Arrays.asList(EdgeTest.class.getDeclaredClasses()));
        testClassesThatContainConsistencyChecks.add(GraphTest.class);
        testClassesThatContainConsistencyChecks.add(GraphComputerTest.class);
        testClassesThatContainConsistencyChecks.addAll(Arrays.asList(FeatureSupportTest.class.getDeclaredClasses()));
        testClassesThatContainConsistencyChecks.addAll(Arrays.asList(PropertyTest.class.getDeclaredClasses()));
        testClassesThatContainConsistencyChecks.addAll(Arrays.asList(VariablesTest.class.getDeclaredClasses()));
        testClassesThatContainConsistencyChecks.add(TransactionTest.class);
        testClassesThatContainConsistencyChecks.addAll(Arrays.asList(VertexTest.class.getDeclaredClasses()));
        testClassesThatContainConsistencyChecks.add(VertexPropertyTest.class);
        testClassesThatContainConsistencyChecks.add(CoreTraversalTest.class);

        // implemented exceptions are the classes that potentially contains exception consistency checks.
        final Set<String> implementedExceptions = testClassesThatContainConsistencyChecks.stream()
                .<ExceptionCoverage>flatMap(c -> Stream.of(c.getAnnotationsByType(ExceptionCoverage.class)))
                .flatMap(ec -> Stream.of(ec.methods()).map(m -> String.format("%s#%s", ec.exceptionClass().getName(), m)))
                .collect(Collectors.<String>toSet());

        // evaluate each of the exceptions and find the list of exception methods...assert them against
        // the list of implementedExceptions to make sure they are covered.
        Stream.of(exceptionDefinitionClasses).flatMap(c -> Stream.of(c.getDeclaredMethods()).map((Method m) -> String.format("%s#%s", c.getName(), m.getName())))
                .filter(s -> !ignore.contains(s))
                .forEach(s -> {
                    logger.info(s);
                    assertTrue(implementedExceptions.contains(s));
                });
    }
}
