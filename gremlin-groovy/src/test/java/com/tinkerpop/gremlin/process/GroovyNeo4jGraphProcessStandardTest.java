package com.tinkerpop.gremlin.process;


import com.tinkerpop.gremlin.AbstractGraphProvider;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.commons.configuration.Configuration;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Executes the Gremlin Process Test Suite using the Groovy flavor of Gremlin process, using TinkerGraph as the test
 * graph database to execute traversals over.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GroovyProcessStandardSuite.class)
@ProcessStandardSuite.GraphProviderClass(GroovyNeo4jGraphProcessStandardTest.class)
public class GroovyNeo4jGraphProcessStandardTest extends AbstractGraphProvider {
    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName) {
        return new HashMap<String, Object>() {{
            put("gremlin.graph", Neo4jGraph.class.getName());
            put("gremlin.neo4j.directory", getWorkingDirectory() + File.separator + graphName);
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (null != g) {
            if (g.getFeatures().graph().supportsTransactions())
                g.tx().rollback();
            g.close();
        }

        if (configuration.containsKey("gremlin.neo4j.directory")) {
            // this is a non-in-memory configuration so blow away the directory
            final File graphDirectory = new File(configuration.getString("gremlin.neo4j.directory"));
            deleteDirectory(graphDirectory);
        }
    }
}
