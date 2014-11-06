package com.tinkerpop.gremlin;

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONModule;
import com.tinkerpop.gremlin.structure.io.kryo.GremlinKryo;
import com.tinkerpop.gremlin.structure.strategy.GraphStrategy;
import com.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.commons.configuration.Configuration;
import org.reflections.Reflections;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Those developing Gremlin implementations must provide a GraphProvider implementation so that the
 * different test suites know how to instantiate their implementations.  Implementers may choose to have multiple
 * {@code GraphProvider} implementations to mix and match with multiple test suite implementations.  For example,
 * create one {@code GraphProvider} that has no indices defined and a separate {@code GraphProvider} that has
 * indices.  Then create separate test suite implementations for each {@code GraphProvider}.  This approach will
 * have the test suites executed once for each {@code GraphProvider} ensuring that the {@link Graph} implementation
 * works under multiple configurations.  Consider making these "extra" tests "integration tests" so that they
 * don't have to be executed on every run of the build so as to save time.  Run the "integration tests" periodically
 * to ensure overall compliance.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GraphProvider {

    /**
     * Creates a new {@link com.tinkerpop.gremlin.structure.Graph} instance using the default
     * {@link org.apache.commons.configuration.Configuration} from {@link #standardGraphConfiguration(Class, String)}.
     * The default implementation converts the passes the
     */
    default public Graph standardTestGraph(final Class<?> test, final String testMethodName) {
        return GraphFactory.open(standardGraphConfiguration(test, testMethodName));
    }

    /**
     * Creates a new {@link com.tinkerpop.gremlin.structure.Graph} instance from the Configuration object using
     * {@link com.tinkerpop.gremlin.structure.util.GraphFactory}. The assumption here is that the {@code Configuration}
     * has been created by one of the {@link #newGraphConfiguration(String, Class, String)} methods and has therefore
     * already been modified by the implementation as necessary for {@link Graph} creation.
     */
    default public Graph openTestGraph(final Configuration config) {
        return openTestGraph(config, null);
    }

    /**
     * Creates a new {@link com.tinkerpop.gremlin.structure.Graph} instance from the Configuration object using
     * {@link com.tinkerpop.gremlin.structure.util.GraphFactory}. The assumption here is that the {@code Configuration}
     * has been created by one of the {@link #newGraphConfiguration(String, Class, String)} methods and has therefore
     * already been modified by the implementation as necessary for {@link Graph} creation.
     */
    default public Graph openTestGraph(final Configuration config, final GraphStrategy strategy) {
        return null == strategy ? GraphFactory.open(config) : GraphFactory.open(config, strategy);
    }

    /**
     * Gets the {@link org.apache.commons.configuration.Configuration} object that can construct a {@link com.tinkerpop.gremlin.structure.Graph} instance from {@link com.tinkerpop.gremlin.structure.util.GraphFactory}.
     * Note that this method should create a {@link com.tinkerpop.gremlin.structure.Graph} using the {@code graphName} of "standard", meaning it
     * should always return a configuration instance that generates the same {@link com.tinkerpop.gremlin.structure.Graph} from the
     * {@link com.tinkerpop.gremlin.structure.util.GraphFactory}.
     */
    default public Configuration standardGraphConfiguration(final Class<?> test, final String testMethodName) {
        return newGraphConfiguration("standard", test, testMethodName, Collections.<String, Object>emptyMap());
    }

    /**
     * If possible (usually with persisted graph) clear the space on disk given the configuration that would be used
     * to construct the graph.  The default implementation simply calls
     * {@link #clear(com.tinkerpop.gremlin.structure.Graph, org.apache.commons.configuration.Configuration)} with
     * a null graph argument.
     */
    public default void clear(final Configuration configuration) throws Exception {
        clear(null, configuration);
    }

    /**
     * Clears a {@link com.tinkerpop.gremlin.structure.Graph} of all data and settings.  Implementations will have
     * different ways of handling this. For a brute force approach, implementers can simply delete data directories
     * provided in the configuration. Implementers may choose a more elegant approach if it exists.
     * <br/>
     * Implementations should be able to accept an argument of null for the Graph, in which case the only action
     * that can be performed is a clear given the configuration.  The method will typically be called this way
     * as clean up task on setup to ensure that a persisted graph has a clear space to create a test graph.
     */
    public void clear(final Graph g, final Configuration configuration) throws Exception;

    /**
     * Converts an identifier from a test to an identifier accepted by the Graph instance.  Test that try to
     * utilize an Element identifier will pass it to this method before usage.
     */
    default public Object convertId(final Object id) {
        return id;
    }

    /**
     * Converts an label from a test to an label accepted by the Graph instance.  Test that try to
     * utilize a label will pass it to this method before usage.
     */
    default public String convertLabel(final String label) {
        return label;
    }

    /**
     * When implementing this method ensure that a test suite can override any settings EXCEPT the
     * "gremlin.graph" setting which should be defined by the implementer. It should provide a
     * {@link org.apache.commons.configuration.Configuration} that will generate a graph unique to that {@code graphName}.
     *
     * @param graphName a unique test graph name
     * @param test the test class
     * @param testMethodName the name of the test
     * @param configurationOverrides settings to override defaults with.
     */
    public Configuration newGraphConfiguration(final String graphName,
                                               final Class<?> test,
                                               final String testMethodName,
                                               final Map<String, Object> configurationOverrides);

    /**
     * When implementing this method ensure that a test suite can override any settings EXCEPT the
     * "gremlin.graph" setting which should be defined by the implementer. It should provide a
     * {@link org.apache.commons.configuration.Configuration} that will generate a graph unique to that {@code graphName}.
     *
     * @param graphName a unique test graph name
     * @param test the test class
     * @param testMethodName the name of the test
     */
    default public Configuration newGraphConfiguration(final String graphName,
                                                       final Class<?> test,
                                                       final String testMethodName) {
        return newGraphConfiguration(graphName, test, testMethodName, new HashMap<>());
    }

    /**
     * Tests are annotated with a {@link com.tinkerpop.gremlin.LoadGraphWith} annotation. These annotations tell
     * the test what kind of data to preload into the graph instance.  It is up to the implementation to load the
     * graph with the data specified by that annotation. This method also represents the place where indices should
     * be configured according the the {@link Graph} implementation's API.
     *
     * @param g             the {@link Graph} instance to load data into constructed by this {@code GraphProvider}
     * @param loadGraphWith the annotation for the currently running test
     */
    public void loadGraphData(final Graph g, final LoadGraphWith loadGraphWith);


    /**
     * Construct a configured {@link GremlinKryo} instance.  The default implementation simply returns the most
     * current version of the default {@link GremlinKryo} configuration.  This object should be satisfactory for
     * most implementations.
     * <br/>
     * The only reason to override this method is if the {@link Graph} implementation utilizes custom classes
     * somewhere that will be serialized in the course of test suite execution.  The most common issue with respect
     * to this situation is the serialization of {@link Element} identifiers that are returned as custom classes.
     * If an implementation does that, then the implementer will want to construct a {@link GremlinKryo} instance
     * and register serializers to it prior to returning it from this method.
     */
    public default GremlinKryo createConfiguredGremlinKryo() {
        return GremlinKryo.build().create();
    }

    /**
     * Construct a configured {@code SimpleModule} instance.  The default implementation simply returns null, which
     * does not apply any additional implementation specific serializers or behaviors.  This return value should
     * be satisfactory for most implementations.
     * <br/>
     * The only reason to override this method is if the {@link Graph} implementation utilizes custom classes
     * somewhere that will be serialized in the course of test suite execution.  The most common issue with respect
     * to this situation is the serialization of {@link Element} identifiers that are returned as custom classes.
     * If an implementation does that, then the implementer will want to construct a {@code SimpleModule} instance
     * with a registered a custom serializer returning it from this method.
     */
    public default SimpleModule createConfiguredGraphSONModule() {
        return null;
    }

    /**
     * Converts the GraphSON representation of an identifier to the implementation's representation of an identifier.
     * When serializing a custom identifier type to GraphSON an implementer will typically specify a custom serializer
     * in {@link #createConfiguredGraphSONModule()}.  That will serialize the identifier to a GraphSON representation.
     * When the GraphSON is deserialized, the identifier is written to an
     * {@link com.tinkerpop.gremlin.structure.util.detached.Attachable} object where it is passed to a user supplied
     * conversion {@link java.util.function.Function} that ultimately processes it.  It is in this conversion process
     * that vendor specific identifier conversion would occur (if desired).  This method mimics that conversion by
     * providing the mechanism that a test can use to do the conversion.
     *
     * @param clazz The {@link Element} class that represents the identifier.
     * @param id The identifier to convert.
     * @param <ID> The type of the identifier.
     * @return The reconstituted identifier.
     */
    public default <ID> ID reconstituteGraphSONIdentifier(final Class<? extends Element> clazz, final Object id) {
        return (ID) id;
    }

    /**
     * Get the set of concrete implementations of certain classes and interfaces utilized by the test suite. The
     * default implementation utilizes reflection given the package name of the {@code GraphProvider} interface
     * as the root for its search, to find implementations of the classes the test suite requires.
     * <br/>
     * This class wants any implementations or extensions of the following interfaces or classes:
     * <ul>
     *     <li>{@link Graph}</li>
     *     <li>{@link Property}</li>
     *     <li>{@link Element}</li>
     *     <li>{@link Traversal}</li>
     *     <li>{@link Traverser}</li>
     *     <li>{@link GraphTraversal}</li>
     *     <li>{@link DefaultGraphTraversal}</li>
     * </ul>
     * <br/>
     * If so desired, implementers can override this method and simply supply the specific classes that implement
     * and extend the above.  This may be necessary if the implementers package structure doesn't align with the
     * default implementation or for some reason the reflection approach is unable to properly get all of the
     * classes required.  It should be clear that a custom implementation of this method is required if there are
     * failures in the GroovyEnvironmentSuite.
     */
    public default Set<Class> getImplementations() {
        final Reflections reflections = new Reflections(this.getClass().getPackage().getName());

        final Set<Class> implementations = new HashSet<>();
        reflections.getSubTypesOf(Graph.class).forEach(implementations::add);
        reflections.getSubTypesOf(Property.class).forEach(implementations::add);
        reflections.getSubTypesOf(Element.class).forEach(implementations::add);
        reflections.getSubTypesOf(Traversal.class).forEach(implementations::add);
        reflections.getSubTypesOf(Traverser.class).forEach(implementations::add);
        reflections.getSubTypesOf(GraphTraversal.class).forEach(implementations::add);
        reflections.getSubTypesOf(DefaultGraphTraversal.class).forEach(implementations::add);

        return implementations.stream()
                .filter(c -> !c.isInterface())
                .collect(Collectors.toSet());
    }
}
