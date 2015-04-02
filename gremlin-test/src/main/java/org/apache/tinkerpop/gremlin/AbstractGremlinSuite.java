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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base Gremlin test suite from which different classes of tests can be exposed to implementers.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinSuite extends Suite {

    // todo: perhaps there is a test that validates against the implementations to be sure that the Graph constructed matches what's defined???
    private static final Set<Class> STRUCTURE_INTERFACES = new HashSet<Class>() {{
        add(Edge.class);
        add(Element.class);
        add(Graph.class);
        add(Graph.Variables.class);
        add(Property.class);
        add(Vertex.class);
        add(VertexProperty.class);
    }};

    /**
     * The GraphProvider instance that will be used to generate a Graph instance.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface GraphProviderClass {
        /**
         * The class of the {@link Graph} that will be returned by the {@link GraphProvider}
         */
        public Class<? extends Graph> graph();

        /**
         * The class of the {@link GraphProvider} implementation to use to generate the {@link Graph} specified by
         * {@link #graph()}
         */
        public Class<? extends GraphProvider> provider();
    }

    /**
     * Indicates that this suite is for testing a gremlin flavor and is therefore not responsible for validating
     * the suite against what the Graph implementation opts-in for.
     */
    private final boolean gremlinFlavorSuite;

    public AbstractGremlinSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute) throws InitializationError {
        this(klass, builder, testsToExecute, null);
    }

    public AbstractGremlinSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute, final Class<?>[] testsToEnforce) throws InitializationError {
        this(klass, builder, testsToExecute, testsToEnforce, false);
    }

    public AbstractGremlinSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute, final Class<?>[] testsToEnforce,
                                final boolean gremlinFlavorSuite) throws InitializationError {
        this(klass, builder, testsToExecute, testsToEnforce, gremlinFlavorSuite, TraversalEngine.Type.STANDARD);
    }

    public AbstractGremlinSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute, final Class<?>[] testsToEnforce,
                                final boolean gremlinFlavorSuite, TraversalEngine.Type traversalEngineType) throws InitializationError {
        super(builder, klass, enforce(testsToExecute, testsToEnforce));

        this.gremlinFlavorSuite = gremlinFlavorSuite;

        // figures out what the implementer assigned as the GraphProvider class and make it available to tests.
        // the klass is the Suite that implements this suite (e.g. GroovyTinkerGraphProcessStandardTest).
        // this class should be annotated with GraphProviderClass.  Failure to do so will toss an InitializationError
        final Pair<Class<? extends GraphProvider>, Class<? extends Graph>> pair = getGraphProviderClass(klass);

        // validate public acknowledgement of the test suite and filter out tests ignored by the implementation
        validateOptInToSuite(pair.getValue1());
        validateOptInAndOutAnnotationsOnGraph(pair.getValue1());
        registerOptOuts(pair.getValue1());

        try {
            final GraphProvider graphProvider = pair.getValue0().newInstance();
            validateStructureInterfacesRegistered(graphProvider);
            validateHelpersNotImplemented(graphProvider);

            GraphManager.setGraphProvider(graphProvider);
            GraphManager.setTraversalEngineType(traversalEngineType);
        } catch (Exception ex) {
            throw new InitializationError(ex);
        }
    }

    /**
     * Need to validate that structure interfaces are implemented so that checks to {@link Graph.Helper} can be
     * properly enforced.
     */
    private void validateStructureInterfacesRegistered(final GraphProvider graphProvider) throws Exception {
        final Set<Class> implementations = graphProvider.getImplementations();
        final Set<Class> noImplementationRegistered = new HashSet<>();

        final Configuration conf = graphProvider.newGraphConfiguration("prototype", AbstractGremlinSuite.class, "validateStructureInterfacesRegistered");
        final Graph graph = graphProvider.openTestGraph(conf);
        final Set<Class> structureInterfaces = new HashSet<>(STRUCTURE_INTERFACES);

        // not all graphs implement all features and therefore may not have implementations of certain "core" interfaces
        if (!graph.features().graph().variables().supportsVariables()) structureInterfaces.remove(Graph.Variables.class);

        graphProvider.clear(graph, conf);

        final boolean missingImplementations = structureInterfaces.stream().anyMatch(iface -> {
            final boolean noneMatch = implementations.stream().noneMatch(c -> iface.isAssignableFrom(c));
            if (noneMatch) noImplementationRegistered.add(iface);
            return noneMatch;
        });

        if (missingImplementations)
            throw new RuntimeException(String.format(
                    "Implementations must register their implementations for the following interfaces %s",
                    String.join(",", noImplementationRegistered.stream().map(Class::getName).collect(Collectors.toList()))));
    }

    private void validateHelpersNotImplemented(final GraphProvider graphProvider) {
        final List<String> overridenMethods = new ArrayList<>();
        graphProvider.getImplementations().forEach(clazz ->
                        Stream.of(clazz.getDeclaredMethods())
                                .filter(AbstractGremlinSuite::isHelperMethodOverriden)
                                .map(m -> m.getDeclaringClass().getName() + "." + m.getName())
                                .forEach(overridenMethods::add)
        );

        if (overridenMethods.size() > 0)
            throw new RuntimeException(String.format(
                    "Implementations cannot override methods marked by @Helper annotation - check the following methods [%s]",
                    String.join(",", overridenMethods)));
    }

    private void validateOptInToSuite(final Class<? extends Graph> klass) throws InitializationError {
        final Graph.OptIn[] optIns = klass.getAnnotationsByType(Graph.OptIn.class);
        if (!gremlinFlavorSuite && !Arrays.stream(optIns).anyMatch(optIn -> optIn.value().equals(this.getClass().getCanonicalName())))
            throw new InitializationError("The suite will not run for this Graph until it is publicly acknowledged with the @OptIn annotation on the Graph instance itself");
    }

    private void registerOptOuts(final Class<? extends Graph> klass) throws InitializationError {
        final Graph.OptOut[] optOuts = klass.getAnnotationsByType(Graph.OptOut.class);

        if (optOuts != null && optOuts.length > 0) {
            // validate annotation - test class and reason must be set
            if (!Arrays.stream(optOuts).allMatch(ignore -> ignore.test() != null && ignore.reason() != null && !ignore.reason().isEmpty()))
                throw new InitializationError("Check @IgnoreTest annotations - all must have a 'test' and 'reason' set");

            try {
                filter(new OptOutTestFilter(optOuts));
            } catch (NoTestsRemainException ex) {
                throw new InitializationError(ex);
            }
        }
    }

    private static Class<?>[] enforce(final Class<?>[] testsToExecute, final Class<?>[] testsToEnforce) {
        if (null == testsToEnforce) return testsToExecute;

        // examine each test to enforce and ensure an instance of it is in the list of testsToExecute
        final List<Class<?>> notSupplied = Stream.of(testsToEnforce)
                .filter(t -> Stream.of(testsToExecute).noneMatch(t::isAssignableFrom))
                .collect(Collectors.toList());

        if (notSupplied.size() > 0)
            System.err.println(String.format("Review the testsToExecute given to the test suite as the following are missing: %s", notSupplied));

        return testsToExecute;
    }

    public static boolean isHelperMethodOverriden(final Method myMethod) {
        final Class<?> declaringClass = myMethod.getDeclaringClass();
        for (Class<?> iface : declaringClass.getInterfaces()) {
            try {
                return iface.getMethod(myMethod.getName(), myMethod.getParameterTypes()).isAnnotationPresent(Graph.Helper.class);
            } catch (NoSuchMethodException ignored) {
            }
        }

        return false;
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
     * Filter for tests in the suite which is controlled by the {@link Graph.OptOut} annotation.
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

        public OptOutTestFilter(final Graph.OptOut[] optOuts) {
            // split the tests to filter into two groups - true represents those that should ignore a whole
            final Map<Boolean, List<Graph.OptOut>> split = Arrays.stream(optOuts).collect(
                    Collectors.groupingBy(optOut -> optOut.method().equals("*")));

            final List<Graph.OptOut> optOutsOfIndividualTests = split.getOrDefault(Boolean.FALSE, Collections.emptyList());
            individualSpecificTestsToIgnore = optOutsOfIndividualTests.stream()
                    .filter(ignoreTest -> !ignoreTest.method().equals("*"))
                    .filter(ignoreTest -> {
                        try {
                            final Class testClass = Class.forName(ignoreTest.test());
                            return !Modifier.isAbstract(testClass.getModifiers());
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    })
                    .<Pair>map(ignoreTest -> Pair.with(ignoreTest.test(), ignoreTest.specific().isEmpty() ? ignoreTest.method() : String.format("%s[%s]", ignoreTest.method(), ignoreTest.specific())))
                    .<Description>map(p -> Description.createTestDescription(p.getValue0().toString(), p.getValue1().toString()))
                    .collect(Collectors.toList());

            testGroupToIgnore = optOutsOfIndividualTests.stream()
                    .filter(ignoreTest -> !ignoreTest.method().equals("*"))
                    .filter(ignoreTest -> {
                        try {
                            final Class testClass = Class.forName(ignoreTest.test());
                            return Modifier.isAbstract(testClass.getModifiers());
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    })
                    .<Pair>map(ignoreTest -> Pair.with(ignoreTest.test(), ignoreTest.specific().isEmpty() ? ignoreTest.method() : String.format("%s[%s]", ignoreTest.method(), ignoreTest.specific())))
                    .<Description>map(p -> Description.createTestDescription(p.getValue0().toString(), p.getValue1().toString()))
                    .collect(Collectors.toList());

            entireTestCaseToIgnore = split.getOrDefault(Boolean.TRUE, Collections.emptyList());
        }

        @Override
        public boolean shouldRun(final Description description) {
            // first check if all tests from a class should be ignored.
            if (!entireTestCaseToIgnore.isEmpty() && entireTestCaseToIgnore.stream().map(optOut -> {
                try {
                    return Class.forName(optOut.test());
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }).anyMatch(claxx -> claxx.isAssignableFrom(description.getTestClass()))) {
                return false;
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
    }
}
