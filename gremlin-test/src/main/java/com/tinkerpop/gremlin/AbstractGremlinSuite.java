package com.tinkerpop.gremlin;

import com.tinkerpop.gremlin.structure.Graph;
import org.junit.runner.Description;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * Base Gremlin test suite from which different classes of tests can be exposed to implementers.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinSuite extends Suite {

    /**
     * The GraphProvider instance that will be used to generate a Graph instance.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface GraphProviderClass {
        public Class<? extends GraphProvider> value();
    }

    /**
     * Defines a test in the suite that the implementer does not want to run.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Repeatable(IgnoreTests.class)
    @Inherited
    public @interface IgnoreTest {
        public Class<?> test();
        public String method();
        public String reason();
    }

    /**
     * Holds a collection of {@link IgnoreTest} enabling multiple {@link IgnoreTest} to be applied to a
     * single suite.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface IgnoreTests {
        IgnoreTest[] value();
    }

    public AbstractGremlinSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute) throws InitializationError {
        this(klass, builder, testsToExecute, null);
    }

    public AbstractGremlinSuite(final Class<?> klass, final RunnerBuilder builder, final Class<?>[] testsToExecute, final Class<?>[] testsToEnforce) throws InitializationError {
        super(builder, klass, enforce(testsToExecute, testsToEnforce));

        // filter out tests ignored by the implementation
        registerIgnoredTests(klass);

        // figures out what the implementer assigned as the GraphProvider class and make it available to tests.
        // the klass is the Suite that implements this suite (e.g. GroovyTinkerGraphProcessStandardTest).
        // this class should be annotated with GraphProviderClass.  Failure to do so will toss an InitializationError
        final Class graphProviderClass = getGraphProviderClass(klass);
        try {
            GraphManager.set((GraphProvider) graphProviderClass.newInstance());
        } catch (Exception ex) {
            throw new InitializationError(ex);
        }
    }

    private void registerIgnoredTests(final Class<?> klass) throws InitializationError {
        final IgnoreTest[] ignoreTests = klass.getAnnotationsByType(IgnoreTest.class);

        if (ignoreTests != null && ignoreTests.length > 0) {
            // validate annotation - test class and reason must be set
            if (!Arrays.stream(ignoreTests).allMatch(ignore -> ignore.test() != null && ignore.reason() != null && !ignore.reason().isEmpty()))
                throw new InitializationError("Check @IgnoreTest annotations - all must have a 'test' and 'reason' set");

            try {
                filter(new IgnoresFilter(ignoreTests));
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

    public static Class<? extends GraphProvider> getGraphProviderClass(final Class<?> klass) throws InitializationError {
        final GraphProviderClass annotation = klass.getAnnotation(GraphProviderClass.class);
        if (null == annotation)
            throw new InitializationError(String.format("class '%s' must have a GraphProviderClass annotation", klass.getName()));
        return annotation.value();
    }

    public static Consumer<Graph> assertVertexEdgeCounts(final int expectedVertexCount, final int expectedEdgeCount) {
        return (g) -> {
            assertEquals(new Long(expectedVertexCount), g.V().count().next());
            assertEquals(new Long(expectedEdgeCount), g.E().count().next());
        };
    }

    /**
     * Filter for tests in the suite which is controlled by the {@link IgnoreTest} annotation.
     */
    public static class IgnoresFilter extends Filter {

        private final List<Description> testsToIgnore;

        public IgnoresFilter(final IgnoreTest[] ignoreTests) {
            testsToIgnore = Arrays.stream(ignoreTests)
                    .<Description>map(ignoreTest -> Description.createTestDescription(ignoreTest.test(), ignoreTest.method()))
                    .collect(Collectors.toList());
        }

        @Override
        public boolean shouldRun(final Description description) {
            if (description.isTest()) {
                return !testsToIgnore.contains(description);
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
                    String.join(",", testsToIgnore.stream().map(Description::getDisplayName).collect(Collectors.toList())));
        }
    }
}
