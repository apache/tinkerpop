package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.util.GraphFactory;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collections;
import java.util.Map;

/**
 * The BlueprintsSuite is a custom JUnit test runner that executes the Blueprints Test Suite over a Graph
 * implementation.  This specialized test suite and runner is for use by Blueprints implementors to test their
 * Graph implementations.  The BlueprintsSuite ensures consistency and validity of the implementations that they
 * test.
 * To use the BlueprintSuite define a class in a test module.  Standard naming would expect the name of the
 * implementation followed by "BlueprintsSuite".  This class should be annotated as follows (note that the "Suite"
 * implements BlueprintsSuite.GraphProvider as a convenience only...it could be implemented in a separate class file):
 * <code>
 *
 * @RunWith(BlueprintsSuite.class)
 * @BlueprintsSuite.GraphProviderClass(MsAccessBlueprintsSuite.class) public class MsAccessBlueprintsSuite implements BlueprintsSuite.GraphProvider {
 * }
 * </code>
 * Implementing BlueprintsSuite.GraphProvider provides a way for the BlueprintsSuite to instantiate Graph instances
 * from the implementation being tested to inject into tests in the suite.  The BlueprintsSuite will utilized
 * Features defined in the suite to determine which tests will be executed.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BlueprintsSuite extends Suite {

    /**
     * This list of tests in the suite that will be executed.  Blueprints developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] testsToExecute = new Class<?>[]{GraphTest.class, IOTest.class};

    /**
     * The GraphProvider instance that will be used to generate a Graph instance.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface GraphProviderClass {
        public Class<? extends GraphProvider> value();
    }

    public BlueprintsSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(builder, klass, testsToExecute);

        // figures out what the implementor assigned as the GraphProvider class and make it available to tests.
        final Class graphProviderClass = getGraphProviderClass(klass);
        try {
            GraphManager.set((GraphProvider) graphProviderClass.newInstance());
        } catch (Exception ex) {
            throw new InitializationError(ex);
        }
    }

    private static Class<? extends GraphProvider> getGraphProviderClass(Class<?> klass) throws InitializationError {
        GraphProviderClass annotation = klass.getAnnotation(GraphProviderClass.class);
        if (annotation == null) {
            throw new InitializationError(String.format("class '%s' must have a GraphProviderClass annotation", klass.getName()));
        }
        return annotation.value();
    }

    /**
     * Those developing Blueprints implementations must provide a GraphProvider implementation so that the
     * BlueprintsSuite knows how to instantiate their implementations.
     */
    public static interface GraphProvider {

        default public Graph newTestGraph() {
            return GraphFactory.open(newGraphConfiguration());
        }

        default public Configuration newGraphConfiguration() {
            return newGraphConfiguration(Collections.<String, Object>emptyMap());
        }

        /**
         * When implementing this method ensure that the BlueprintsSuite can override any settings EXCEPT the
         * "blueprints.graph" setting which should be defined by the implementor.
         *
         * @param configurationOverrides Settings to override defaults with.
         */
        public Configuration newGraphConfiguration(Map<String, Object> configurationOverrides);
    }

    /**
     * A basic GraphProvider which simply requires the implementor to supply their base configuration for their
     * Graph instance.  Minimally this is just the setting for "blueprints.graph".
     */
    public static abstract class AbstractGraphProvider implements GraphProvider {

        public abstract Map<String, Object> getBaseConfiguration();

        @Override
        public Configuration newGraphConfiguration(final Map<String, Object> configurationOverrides) {
            final Configuration conf = new BaseConfiguration();
            getBaseConfiguration().entrySet().stream()
                    .forEach(e -> conf.setProperty(e.getKey(), e.getValue()));

            // assign overrides but don't allow blueprints.graph setting to be overriden.  the test suite should
            // not be able to override that.
            configurationOverrides.entrySet().stream()
                    .filter(c -> !c.getKey().equals("blueprints.graph"))
                    .forEach(e -> conf.setProperty(e.getKey(), e.getValue()));
            return conf;
        }
    }

    /**
     * Holds the GraphProvider specified by the implementor using the BlueprintSuite.
     */
    public static class GraphManager {
        private static GraphProvider graphProvider;

        public static GraphProvider set(final GraphProvider graphProvider) {
            final GraphProvider old = GraphManager.graphProvider;
            GraphManager.graphProvider = graphProvider;
            return old;
        }

        public static GraphProvider get() {
            return graphProvider;
        }
    }
}
