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

import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.javatuples.Pair;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base Gremlin test suite from which different classes of tests can be exposed to implementers.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinSuite extends Suite {
    private static final Logger logger = LoggerFactory.getLogger(AbstractGremlinSuite.class);
    /**
     * Indicates that this suite is for testing a gremlin flavor and is therefore not responsible for validating
     * the suite against what the {@link Graph} implementation opts-in for. This setting will let Gremlin flavor
     * developers run their test cases against a {@link Graph} without the need for the {@link Graph} to supply
     * an {@link org.apache.tinkerpop.gremlin.structure.Graph.OptIn} annotation.  Not having that annotation is a
     * likely case for flavors while they are under development and a
     * {@link org.apache.tinkerpop.gremlin.structure.Graph.OptIn} is not possible.
     */
    private final boolean gremlinFlavorSuite;

    /**
     * Constructs a Gremlin Test Suite implementation.
     *
     * @param klass               Required for JUnit Suite construction
     * @param builder             Required for JUnit Suite construction
     * @param testsToExecute      The list of tests to execute
     * @param testsToEnforce      The list of tests to "enforce" such that a check is made to ensure that in this list,
     *                            there exists an implementation in the testsToExecute (use {@code null} for no
     *                            enforcement).
     * @param gremlinFlavorSuite  Ignore validation of {@link org.apache.tinkerpop.gremlin.structure.Graph.OptIn}
     *                            annotations which is typically reserved for structure tests
     * @param traversalEngineType The {@link org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine.Type} to
     *                            enforce on this suite
     */
    public AbstractGremlinSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute,
                                final Class<?>[] testsToEnforce, final boolean gremlinFlavorSuite,
                                final TraversalEngine.Type traversalEngineType) throws InitializationError {
        super(builder, klass, enforce(testsToExecute, testsToEnforce));

        this.gremlinFlavorSuite = gremlinFlavorSuite;

        // figures out what the implementer assigned as the GraphProvider class and make it available to tests.
        // the klass is the Suite that implements this suite (e.g. GroovyTinkerGraphProcessStandardTest).
        // this class should be annotated with GraphProviderClass.  Failure to do so will toss an InitializationError
        final Pair<Class<? extends GraphProvider>, Class<? extends Graph>> pair = getGraphProviderClass(klass);

        // the GraphProvider.Descriptor is only needed right now if the test if for a computer engine - an
        // exception is thrown if it isn't present.
        final Optional<GraphProvider.Descriptor> graphProviderDescriptor = getGraphProviderDescriptor(traversalEngineType, pair.getValue0());

        // validate public acknowledgement of the test suite and filter out tests ignored by the implementation
        validateOptInToSuite(pair.getValue1());
        validateOptInAndOutAnnotationsOnGraph(pair.getValue1());

        registerOptOuts(pair.getValue1(), graphProviderDescriptor, traversalEngineType);

        try {
            final GraphProvider graphProvider = pair.getValue0().newInstance();
            GraphManager.setGraphProvider(graphProvider);
            GraphManager.setTraversalEngineType(traversalEngineType);
        } catch (Exception ex) {
            throw new InitializationError(ex);
        }
    }

    private Optional<GraphProvider.Descriptor> getGraphProviderDescriptor(final TraversalEngine.Type traversalEngineType,
                                                                          final Class<? extends GraphProvider> klass) throws InitializationError {
        final GraphProvider.Descriptor descriptorAnnotation = klass.getAnnotation(GraphProvider.Descriptor.class);
        if (traversalEngineType == TraversalEngine.Type.COMPUTER) {
            // can't be null if this is graph computer business
            if (null == descriptorAnnotation)
                throw new InitializationError(String.format("For 'computer' tests, '%s' must have a GraphProvider.Descriptor annotation", klass.getName()));
        }

        return Optional.ofNullable(descriptorAnnotation);
    }

    private void validateOptInToSuite(final Class<? extends Graph> klass) throws InitializationError {
        final Graph.OptIn[] optIns = klass.getAnnotationsByType(Graph.OptIn.class);
        if (!Arrays.stream(optIns).anyMatch(optIn -> optIn.value().equals(this.getClass().getCanonicalName())))
            if (gremlinFlavorSuite)
                logger.error(String.format("The %s will run for this Graph as it is testing a Gremlin flavor but the Graph does not publicly acknowledged it yet with the @OptIn annotation.", this.getClass().getSimpleName()));
            else
                throw new InitializationError(String.format("The %s will not run for this Graph until it is publicly acknowledged with the @OptIn annotation on the Graph instance itself", this.getClass().getSimpleName()));
    }

    private void registerOptOuts(final Class<? extends Graph> graphClass,
                                 final Optional<GraphProvider.Descriptor> graphProviderDescriptor,
                                 final TraversalEngine.Type traversalEngineType) throws InitializationError {
        final Graph.OptOut[] optOuts = graphClass.getAnnotationsByType(Graph.OptOut.class);

        if (optOuts != null && optOuts.length > 0) {
            // validate annotation - test class and reason must be set
            if (!Arrays.stream(optOuts).allMatch(ignore -> ignore.test() != null && ignore.reason() != null && !ignore.reason().isEmpty()))
                throw new InitializationError("Check @IgnoreTest annotations - all must have a 'test' and 'reason' set");

            try {
                filter(new OptOutTestFilter(optOuts, graphProviderDescriptor, traversalEngineType));
            } catch (NoTestsRemainException ex) {
                throw new InitializationError(ex);
            }
        }
    }

    private static Class<?>[] enforce(final Class<?>[] unfilteredTestsToExecute, final Class<?>[] unfilteredTestsToEnforce) {
        // Allow env var to filter the test lists.
        final Class<?>[] testsToExecute = filterSpecifiedTests(unfilteredTestsToExecute);
        final Class<?>[] testsToEnforce = filterSpecifiedTests(unfilteredTestsToEnforce);

        // If there are no tests specified to enforce, enforce them all.
        if (null == testsToEnforce) return testsToExecute;

        // examine each test to enforce and ensure an instance of it is in the list of testsToExecute
        final List<Class<?>> notSupplied = Stream.of(testsToEnforce)
                .filter(t -> Stream.of(testsToExecute).noneMatch(t::isAssignableFrom))
                .collect(Collectors.toList());

        if (notSupplied.size() > 0)
            logger.error(String.format("Review the testsToExecute given to the test suite as the following are missing: %s", notSupplied));

        return testsToExecute;
    }

    /**
     * Filter a list of test classes through the GREMLIN_TESTS environment variable list.
     */
    private static Class<?>[] filterSpecifiedTests(final Class<?>[] allTests) {
        if (null == allTests) return null;

        Class<?>[] filteredTests;
        final String override = System.getenv().getOrDefault("GREMLIN_TESTS", "");
        if (override.equals(""))
            filteredTests = allTests;
        else {
            final List<String> filters = Arrays.asList(override.split(","));
            final List<Class<?>> allowed = Stream.of(allTests)
                    .filter(c -> filters.contains(c.getName()))
                    .collect(Collectors.toList());
            filteredTests = allowed.toArray(new Class<?>[allowed.size()]);
        }
        return filteredTests;
    }

    public static Pair<Class<? extends GraphProvider>, Class<? extends Graph>> getGraphProviderClass(final Class<?> klass) throws InitializationError {
        final GraphProviderClass annotation = klass.getAnnotation(GraphProviderClass.class);
        if (null == annotation)
            throw new InitializationError(String.format("class '%s' must have a GraphProviderClass annotation", klass.getName()));
        return Pair.with(annotation.provider(), annotation.graph());
    }

    public static void validateOptInAndOutAnnotationsOnGraph(final Class<? extends Graph> klass) throws InitializationError {
        // sometimes test names change and since they are String representations they can easily break if a test
        // is renamed. this test will validate such things.  it is not possible to @OptOut of this test.
        final Graph.OptOut[] optOuts = klass.getAnnotationsByType(Graph.OptOut.class);
        for (Graph.OptOut optOut : optOuts) {
            final Class testClass;
            try {
                testClass = Class.forName(optOut.test());
            } catch (Exception ex) {
                throw new InitializationError(String.format("Invalid @OptOut on Graph instance.  Could not instantiate test class (it may have been renamed): %s", optOut.test()));
            }

            if (!optOut.method().equals("*") && !Arrays.stream(testClass.getMethods()).anyMatch(m -> m.getName().equals(optOut.method())))
                throw new InitializationError(String.format("Invalid @OptOut on Graph instance.  Could not match @OptOut test name %s on test class %s (it may have been renamed)", optOut.method(), optOut.test()));
        }
    }

    @Override
    protected void runChild(final Runner runner, final RunNotifier notifier) {
        if (beforeTestExecution((Class<? extends AbstractGremlinTest>) runner.getDescription().getTestClass()))
            super.runChild(runner, notifier);
        afterTestExecution((Class<? extends AbstractGremlinTest>) runner.getDescription().getTestClass());
    }

    /**
     * Called just prior to test class execution.  Return false to ignore test class. By default this always returns
     * true.
     */
    public boolean beforeTestExecution(final Class<? extends AbstractGremlinTest> testClass) {
        return true;
    }

    /**
     * Called just after test class execution.
     */
    public void afterTestExecution(final Class<? extends AbstractGremlinTest> testClass) {
    }

    /**
     * Filter for tests in the suite which is controlled by the {@link org.apache.tinkerpop.gremlin.structure.Graph.OptOut} annotation.
     */
    public static class OptOutTestFilter extends Filter {

        /**
         * Ignores a specific test in a specific test case.
         */
        private final List<Description> individualSpecificTestsToIgnore;

        /**
         * Defines a group of tests to ignore which is useful with some of the Gremlin process tests which all
         * have the same name.  It is only possible to ignore tests that extend from certain pre-defined classes.
         */
        private final List<Description> testGroupToIgnore;

        /**
         * Ignores an entire specific test case.
         */
        private final List<Graph.OptOut> entireTestCaseToIgnore;

        private final Optional<GraphProvider.Descriptor> graphProviderDescriptor;
        private final TraversalEngine.Type traversalEngineType;

        public OptOutTestFilter(final Graph.OptOut[] optOuts,
                                final Optional<GraphProvider.Descriptor> graphProviderDescriptor,
                                final TraversalEngine.Type traversalEngineType) {
            this.graphProviderDescriptor = graphProviderDescriptor;
            this.traversalEngineType = traversalEngineType;

            // split the tests to filter into two groups - true represents those that should ignore a whole
            final Map<Boolean, List<Graph.OptOut>> split = Arrays.stream(optOuts)
                    .filter(this::checkGraphProviderDescriptorForComputer)
                    .collect(Collectors.groupingBy(optOut -> optOut.method().equals("*")));

            final List<Graph.OptOut> optOutsOfIndividualTests = split.getOrDefault(Boolean.FALSE, Collections.emptyList());
            individualSpecificTestsToIgnore = optOutsOfIndividualTests.stream()
                    .filter(ignoreTest -> !ignoreTest.method().equals("*"))
                    .filter(allowAbstractMethod(false))
                    .<Pair>map(ignoreTest -> Pair.with(ignoreTest.test(), ignoreTest.specific().isEmpty() ? ignoreTest.method() : String.format("%s[%s]", ignoreTest.method(), ignoreTest.specific())))
                    .<Description>map(p -> Description.createTestDescription(p.getValue0().toString(), p.getValue1().toString()))
                    .collect(Collectors.toList());

            testGroupToIgnore = optOutsOfIndividualTests.stream()
                    .filter(ignoreTest -> !ignoreTest.method().equals("*"))
                    .filter(allowAbstractMethod(true))
                    .<Pair>map(ignoreTest -> Pair.with(ignoreTest.test(), ignoreTest.specific().isEmpty() ? ignoreTest.method() : String.format("%s[%s]", ignoreTest.method(), ignoreTest.specific())))
                    .<Description>map(p -> Description.createTestDescription(p.getValue0().toString(), p.getValue1().toString()))
                    .collect(Collectors.toList());

            entireTestCaseToIgnore = split.getOrDefault(Boolean.TRUE, Collections.emptyList());
        }

        @Override
        public boolean shouldRun(final Description description) {
            // first check if all tests from a class should be ignored - where "OptOut.method" is set to "*". the
            // description appears to be null in some cases of parameterized tests, but if the entire test case
            // was ignored it would have been caught earlier and these parameterized tests wouldn't be considered
            // for a call to shouldRun
            if (description.getTestClass() != null) {
                final boolean ignoreWholeTestCase = entireTestCaseToIgnore.stream().map(this::transformToClass)
                        .anyMatch(claxx -> claxx.isAssignableFrom(description.getTestClass()));
                if (ignoreWholeTestCase) return false;
            }

            if (description.isTest()) {
                // next check if there is a test group to consider. if not then check for a  specific test to ignore
                return !(!testGroupToIgnore.isEmpty() && testGroupToIgnore.stream().anyMatch(optOut -> optOut.getTestClass().isAssignableFrom(description.getTestClass()) && description.getMethodName().equals(optOut.getMethodName())))
                        && !individualSpecificTestsToIgnore.contains(description);
            }

            // explicitly check if any children want to run
            for (Description each : description.getChildren()) {
                if (shouldRun(each)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String describe() {
            return String.format("Method %s",
                    String.join(",", individualSpecificTestsToIgnore.stream().map(Description::getDisplayName).collect(Collectors.toList())));
        }

        private Predicate<Graph.OptOut> allowAbstractMethod(final boolean allow) {
            return optOut -> {
                try {
                    // ignore those methods in process that are defined as abstract methods
                    final Class testClass = Class.forName(optOut.test());
                    if (allow)
                        return Modifier.isAbstract(testClass.getModifiers());
                    else
                        return !Modifier.isAbstract(testClass.getModifiers());
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            };
        }

        private boolean checkGraphProviderDescriptorForComputer(final Graph.OptOut optOut) {
            // immediately include the ignore if this is a standard tests suite (i.e. not computer)
            // or if the OptOut doesn't specify any computers to filter
            if (traversalEngineType == TraversalEngine.Type.STANDARD
                    || optOut.computers().length == 0) {
                return true;
            }
            // can assume that that GraphProvider.Descriptor is not null at this point.  a test should
            // only opt out if it matches the expected computer
            return Stream.of(optOut.computers()).map(c -> {
                try {
                    return Class.forName(c);
                } catch (ClassNotFoundException e) {
                    return Object.class;
                }
            }).filter(c -> !c.equals(Object.class)).anyMatch(c -> c == graphProviderDescriptor.get().computer());
        }

        private Class<?> transformToClass(final Graph.OptOut optOut) {
            try {
                return Class.forName(optOut.test());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
