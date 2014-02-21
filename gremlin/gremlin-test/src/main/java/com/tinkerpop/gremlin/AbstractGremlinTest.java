package com.tinkerpop.gremlin;

import com.tinkerpop.gremlin.structure.FeatureRequirement;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import com.tinkerpop.gremlin.structure.strategy.GraphStrategy;
import com.tinkerpop.gremlin.structure.strategy.StrategyWrappedGraph;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assume.assumeThat;

/**
 * Sets up g based on the current graph configuration and checks required features for the test.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinTest {
    protected Graph g;
    protected Configuration config;
    protected Optional<? extends GraphStrategy> strategyToTest;
    protected GraphProvider graphProvider;

    @Rule
    public TestName name = new TestName();

    public AbstractGremlinTest() {
        this(Optional.empty());
    }

    public AbstractGremlinTest(final Optional<? extends GraphStrategy> strategyToTest) {
        this.strategyToTest = strategyToTest;
    }

    @Before
    public void setup() throws Exception {
        graphProvider = GraphManager.get();
        config = graphProvider.standardGraphConfiguration();
        g = graphProvider.openTestGraph(config, strategyToTest);

        final Method testMethod = this.getClass().getMethod(cleanMethodName(name.getMethodName()));
        final FeatureRequirement[] featureRequirement = testMethod.getAnnotationsByType(FeatureRequirement.class);
        final List<FeatureRequirement> frs = Arrays.asList(featureRequirement);
        for (FeatureRequirement fr : frs) {
            try {
                assumeThat(g.getFeatures().supports(fr.featureClass(), fr.feature()), is(fr.supported()));
            } catch (NoSuchMethodException nsme) {
                throw new NoSuchMethodException(String.format("[supports%s] is not a valid feature on %s", fr.feature(), fr.featureClass()));
            } catch (Exception ex) {
                throw ex;
            }
        }

        final LoadGraphWith[] loadGraphWiths = testMethod.getAnnotationsByType(LoadGraphWith.class);
        final Optional<LoadGraphWith> loadGraphWith = loadGraphWiths.length == 0 ? Optional.empty() : Optional.of(loadGraphWiths[0]);
        loadGraphWith.ifPresent(lgw -> {
            try {
                readGraphMLIntoGraph(g, lgw.value().location());
            } catch (IOException ioe) {
                throw new RuntimeException("Graph could not be loaded with data for test.");
            }
        });

        prepareGraph(g);
    }

    protected void prepareGraph(final Graph g) throws Exception {
        // do nothing
    }

    @After
    public void tearDown() throws Exception {
        graphProvider.clear(g, config);
    }

    /**
     * Utility method that commits if the graph supports transactions.
     */
    protected void tryCommit(final Graph g) {
        if (g.getFeatures().graph().supportsTransactions())
            g.tx().commit();
    }

    /**
     * Utility method that commits if the graph supports transactions and executes an assertion function before and
     * after the commit.  It assumes that the assertion should be true before and after the commit.
     */
    protected void tryCommit(final Graph g, final Consumer<Graph> assertFunction) {
        assertFunction.accept(g);
        if (g.getFeatures().graph().supportsTransactions()) {
            g.tx().commit();
            assertFunction.accept(g);
        }
    }

    /**
     * Utility method that rollsback if the graph supports transactions.
     */
    protected void tryRollback(final Graph g) {
        if (g.getFeatures().graph().supportsTransactions())
            g.tx().rollback();
    }

    /**
     * If using "parameterized test" junit will append an identifier to the end of the method name which prevents it
     * from being found via reflection.  This method removes that suffix.
     */
    private static String cleanMethodName(final String methodName) {
        if (methodName.endsWith("]")) {
            return methodName.substring(0, methodName.indexOf("["));
        }

        return methodName;
    }

    private static void readGraphMLIntoGraph(final Graph g, final String path) throws IOException {
        final GraphReader reader = new GraphMLReader.Builder(g).build();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream(path)) {
            reader.inputGraph(stream);
        }
    }
}
