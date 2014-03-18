package com.tinkerpop.gremlin;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.io.File;
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
     * the {@code graphName} to ensure that the instance is unique.  It is up to the Gremlin Structure implementation
     * to determine how best to use the {@code graphName} to ensure uniqueness.  For example, Neo4j, might use the
     * {@code graphName} might be used to create a different sub-directory where the graph is stored.
     *
     * @param graphName a value that represents a unique configuration for a graph
     * @return a configuration {@link java.util.Map} that should be unique per the {@code graphName}
     */
    public abstract Map<String, Object> getBaseConfiguration(final String graphName);

    @Override
    public Configuration newGraphConfiguration(final String graphName, final Map<String, Object> configurationOverrides) {
        final Configuration conf = new BaseConfiguration();
        getBaseConfiguration(graphName).entrySet().stream()
                .forEach(e -> conf.setProperty(e.getKey(), e.getValue()));

        // assign overrides but don't allow gremlin.graph setting to be overridden.  the test suite should
        // not be able to override that.
        configurationOverrides.entrySet().stream()
                .filter(c -> !c.getKey().equals("gremlin.graph"))
                .forEach(e -> conf.setProperty(e.getKey(), e.getValue()));
        return conf;
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
}
