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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.engine.StandardTraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.O_Traverser;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

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
     * Implementations from {@code gremlin-core} that need to be part of the clear process.  This does not exempt
     * vendors from having to register their extensions to any of these classes, but does prevent them from
     * having to register them in addition to their own.
     */
    public static final Set<Class> CORE_IMPLEMENTATIONS = new HashSet<Class>() {{
        add(__.class);
        add(DefaultGraphTraversal.class);
        add(GraphTraversalSource.class);
        add(B_O_S_SE_SL_Traverser.class);
        add(B_LP_O_P_S_SE_SL_Traverser.class);
        add(B_LP_O_S_SE_SL_Traverser.class);
        add(B_O_Traverser.class);
        add(O_Traverser.class);
    }};

    /**
     * Create a {@link GraphTraversalSource} from a {@link Graph} instance.  The default implementation uses the
     * {@link StandardTraversalEngine} so vendors should override as necessary if their implementation is testing
     * something that requires a different engine type, like those tests for
     * {@link org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine.Type}.
     */
    public default GraphTraversalSource traversal(final Graph graph) {
        return GraphTraversalSource.standard().create(graph);
    }

    /**
     * Create a {@link GraphTraversalSource} from a {@link Graph} instance.  The default implementation uses the
     * {@link StandardTraversalEngine} so vendors should override as necessary if their implementation is testing
     * something that requires a different engine type, like those tests for
     * {@link org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine.Type}.
     * <p/>
     * Implementations should apply strategies as necessary to the
     * {@link org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource.Builder} before calling
     * it's {@code create} method.
     */
    public default GraphTraversalSource traversal(final Graph graph, final TraversalStrategy... strategies) {
        final GraphTraversalSource.Builder builder = GraphTraversalSource.build().engine(StandardTraversalEngine.build());
        Stream.of(strategies).forEach(builder::with);
        return builder.create(graph);
    }

    /**
     * Creates a new {@link org.apache.tinkerpop.gremlin.structure.Graph} instance using the default
     * {@code org.apache.commons.configuration.Configuration} from
     * {@link #standardGraphConfiguration(Class, String, LoadGraphWith.GraphData)}.
     */
    default public Graph standardTestGraph(final Class<?> test, final String testMethodName, final LoadGraphWith.GraphData loadGraphWith) {
        return GraphFactory.open(standardGraphConfiguration(test, testMethodName, loadGraphWith));
    }

    /**
     * Creates a new {@link Graph} instance from the Configuration object using {@link GraphFactory}. The assumption
     * here is that the {@code Configuration} has been created by one of the
     * {@link #newGraphConfiguration(String, Class, String, LoadGraphWith.GraphData)} methods and has therefore
     * already been modified by the implementation as necessary for {@link Graph} creation.
     */
    default public Graph openTestGraph(final Configuration config) {
        return GraphFactory.open(config);
    }

    /**
     * Gets the {@code Configuration} object that can construct a {@link Graph} instance from {@link GraphFactory}.
     * Note that this method should create a {@link Graph} using the {@code graphName} of "standard", meaning it
     * should always return a configuration instance that generates the same {@link Graph} from the
     * {@link GraphFactory}.
     */
    default public Configuration standardGraphConfiguration(final Class<?> test, final String testMethodName, final LoadGraphWith.GraphData loadGraphWith) {
        return newGraphConfiguration("standard", test, testMethodName, Collections.<String, Object>emptyMap(), loadGraphWith);
    }

    /**
     * If possible (usually with persisted graph) clear the space on disk given the configuration that would be used
     * to construct the graph.  The default implementation simply calls
     * {@link #clear(Graph, org.apache.commons.configuration.Configuration)} with
     * a null graph argument.
     */
    public default void clear(final Configuration configuration) throws Exception {
        clear(null, configuration);
    }

    /**
     * Clears a {@link Graph} of all data and settings.  Implementations will have different ways of handling this.
     * For a brute force approach, implementers can simply delete data directories provided in the configuration.
     * Implementers may choose a more elegant approach if it exists.
     * <p/>
     * Implementations should be able to accept an argument of null for the Graph, in which case the only action
     * that can be performed is a clear given the configuration.  The method will typically be called this way
     * as clean up task on setup to ensure that a persisted graph has a clear space to create a test graph.
     */
    public void clear(final Graph graph, final Configuration configuration) throws Exception;

    /**
     * Converts an identifier from a test to an identifier accepted by the {@link Graph} instance.  Test that try to
     * utilize an Element identifier will pass it to this method before usage.  This method should be sure to
     * be consistent in the return value such that calling it with "x" should always return the same transformed
     * value.
     */
    default public Object convertId(final Object id, final Class<? extends Element> c) {
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
     * {@code Configuration} that will generate a graph unique to that {@code graphName}.
     *
     * @param graphName              a unique test graph name
     * @param test                   the test class
     * @param testMethodName         the name of the test
     * @param configurationOverrides settings to override defaults with.
     * @param loadGraphWith          the data set to load and will be null if no data is to be loaded
     */
    public Configuration newGraphConfiguration(final String graphName,
                                               final Class<?> test,
                                               final String testMethodName,
                                               final Map<String, Object> configurationOverrides,
                                               final LoadGraphWith.GraphData loadGraphWith);

    /**
     * When implementing this method ensure that a test suite can override any settings EXCEPT the
     * "gremlin.graph" setting which should be defined by the implementer. It should provide a
     * {@code Configuration} that will generate a graph unique to that {@code graphName}.
     *
     * @param graphName      a unique test graph name
     * @param test           the test class
     * @param testMethodName the name of the test
     * @param loadGraphWith  the data set to load and will be null if no data is to be loaded
     */
    default public Configuration newGraphConfiguration(final String graphName,
                                                       final Class<?> test,
                                                       final String testMethodName,
                                                       final LoadGraphWith.GraphData loadGraphWith) {
        return newGraphConfiguration(graphName, test, testMethodName, new HashMap<>(), loadGraphWith);
    }

    /**
     * Tests are annotated with a {@link LoadGraphWith} annotation. These annotations tell the test what kind of data
     * to preload into the graph instance.  It is up to the implementation to load the graph with the data specified
     * by that annotation. This method also represents the place where indices should be configured according the
     * the {@link Graph} implementation's API. Implementers can use the {@code testClass} and {@code testName}
     * arguments to implement test specific configurations to their graphs.
     *
     * @param graph         the {@link Graph} instance to load data into constructed by this {@code GraphProvider}
     * @param loadGraphWith the annotation for the currently running test - this value may be null if no graph
     *                      data is to be loaded in front of the test.
     * @param testClass     the test class being executed
     * @param testName      the name of the test method being executed
     */
    public void loadGraphData(final Graph graph, final LoadGraphWith loadGraphWith, final Class testClass, final String testName);

    /**
     * Get the set of concrete implementations of certain classes and interfaces utilized by the test suite. This
     * method should return any implementations or extensions of the following interfaces or classes:
     * <ul>
     * <li>{@link Edge}</li>
     * <li>{@link Element}</li>
     * <li>{@link DefaultGraphTraversal}</li>
     * <li>{@link Graph}</li>
     * <li>{@link org.apache.tinkerpop.gremlin.structure.Graph.Variables}</li>
     * <li>{@link GraphTraversal}</li>
     * <li>{@link B_LP_O_P_S_SE_SL_Traverser}</li>
     * <li>{@link Property}</li>
     * <li>{@link B_O_S_SE_SL_Traverser}</li>
     * <li>{@link Traversal}</li>
     * <li>{@link Traverser}</li>
     * <li>{@link Vertex}</li>
     * <li>{@link VertexProperty}</li>
     * </ul>
     * <p/>
     * The test suite only enforces registration of the following core structure interfaces (i.e. these classes must
     * be registered or the tests will fail to execute):
     * <ul>
     * <li>{@link Edge}</li>
     * <li>{@link Element}</li>
     * <li>{@link Graph}</li>
     * <li>{@link org.apache.tinkerpop.gremlin.structure.Graph.Variables}</li>
     * <li>{@link Property}</li>
     * <li>{@link Vertex}</li>
     * <li>{@link VertexProperty}</li>
     * </ul>
     * <p/>
     * The remaining interfaces and classes should be registered however as failure to do so, might cause failures
     * in the Groovy environment testing suite.
     * <p/>
     * Internally speaking, tests that make use of this method should bind in {@link #CORE_IMPLEMENTATIONS} to the
     * {@link Set} because these represent {@code gremlin-core} implementations that are likely not registered
     * by the vendor implementations.
     */
    public Set<Class> getImplementations();

    /**
     * Helper method for those build {@link GraphProvider} implementations that need a standard working directory
     * for tests (e.g. graphs that persist data to disk). Typically, there is no need to override the default
     * behavior of this method and if it is overridden, it is usually best to continue to use the {@link TestHelper}
     * to produce the working directory as it will create the path in the appropriate build directories.
     */
    public default String getWorkingDirectory() {
        return TestHelper.makeTestDataPath(this.getClass(), "graph-provider-data").getAbsolutePath();
    }

    /**
     * An annotation to be applied to a {@code GraphProvider} implementation that provides additional information
     * about its intentions. The {@code Descriptor} is required by those {@code GraphProvider} implementations
     * that will be assigned to test suites that use {@link TraversalEngine.Type#COMPUTER}.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface Descriptor {

        /**
         * The {@link GraphComputer} implementation that the {@code GraphProvider} will use when it constructs
         * a {@link Traversal} with {@link #traversal(Graph)} or {@link #traversal(Graph, TraversalStrategy[])}.
         * This value should be null if a {@link GraphComputer} is not being used.
         */
        public Class<? extends GraphComputer> computer();
    }
}
