package com.tinkerpop.gremlin;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;

/**
 * A basic GraphProvider which simply requires the implementer to supply their base configuration for their
 * Graph instance.  Minimally this is just the setting for "gremlin.graph".
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGraphProvider implements GraphProvider {

    /**
     * Provides a basic configuration for a particular {@link com.tinkerpop.gremlin.structure.Graph} instance and used
     * the {@code graphName} to ensure that the instance is unique.  It is up to the Gremlin implementation
     * to determine how best to use the {@code graphName} to ensure uniqueness.  For example, Neo4j, might use the
     * {@code graphName} might be used to create a different sub-directory where the graph is stored.
     * <p/>
     * The @{code test} and @{code testMethodName} can be used to alter graph configurations for specific tests.
     * For example, a graph that has support for different transaction isolation levels might only support a feature
     * in a specific configuration.  Using these arguments, the implementation could detect when a test was being
     * fired that required the database to be configured in a specific isolation level and return a configuration
     * to support that.
     *
     * @param graphName      a value that represents a unique configuration for a graph
     * @param test           the test class
     * @param testMethodName the name of the test method
     * @return a configuration {@link java.util.Map} that should be unique per the {@code graphName}
     */
    public abstract Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test,
                                                             final String testMethodName);

    @Override
    public Configuration newGraphConfiguration(final String graphName, final Class<?> test,
                                               final String testMethodName,
                                               final Map<String, Object> configurationOverrides) {
        final Configuration conf = new BaseConfiguration();
        getBaseConfiguration(graphName, test, testMethodName).entrySet().stream()
                .forEach(e -> conf.setProperty(e.getKey(), e.getValue()));

        // assign overrides but don't allow gremlin.graph setting to be overridden.  the test suite should
        // not be able to override that.
        configurationOverrides.entrySet().stream()
                .filter(c -> !c.getKey().equals("gremlin.graph"))
                .forEach(e -> conf.setProperty(e.getKey(), e.getValue()));
        return conf;
    }

    @Override
    public void loadGraphData(final Graph g, final LoadGraphWith loadGraphWith) {
        try {
            readIntoGraph(g, loadGraphWith.value().location());
        } catch (IOException ioe) {
            throw new RuntimeException("Graph could not be loaded with data for test: " + ioe.getMessage());
        }
    }

    protected static void deleteDirectory(final File directory) {
        if (directory.exists()) {
            for (File file : directory.listFiles()) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
            directory.delete();
        }

        // overkill code, simply allowing us to detect when data dir is in use.  useful though because without it
        // tests may fail if a database is re-used in between tests somehow.  this directory really needs to be
        // cleared between tests runs and this exception will make it clear if it is not.
        if (directory.exists()) {
            throw new RuntimeException("unable to delete directory " + directory.getAbsolutePath());
        }
    }

    protected String getWorkingDirectory() {
        return this.computeTestDataRoot().getAbsolutePath();
    }

    protected File computeTestDataRoot() {
        final String clsUri = this.getClass().getName().replace('.', '/') + ".class";
        final URL url = this.getClass().getClassLoader().getResource(clsUri);
        final String clsPath = url.getPath();
        final File root = new File(clsPath.substring(0, clsPath.length() - clsUri.length()));
        return new File(root.getParentFile(), "test-data");
    }

    protected static void readIntoGraph(final Graph g, final String path) throws IOException {
        final GraphReader reader = KryoReader.build().setWorkingDirectory(File.separator + "tmp").create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream(path)) {
            reader.readGraph(stream, g);
        }
    }
}
