package com.tinkerpop.gremlin.process;


import com.tinkerpop.gremlin.AbstractGraphProvider;
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
@ProcessStandardSuite.GraphProviderClass(provider = GroovyTinkerGraphProcessStandardTest.class, graph = TinkerGraph.class)
public class GroovyTinkerGraphProcessStandardTest extends AbstractGraphProvider {

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName) {
        return new HashMap<String, Object>() {{
            put("gremlin.graph", TinkerGraph.class.getName());
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (g != null) g.close();

        if (configuration.containsKey("gremlin.tg.directory")) {
            // this is a non-in-sideEffects configuration so blow away the directory
            final File graphDirectory = new File(configuration.getString("gremlin.tg.directory"));
            graphDirectory.delete();
        }
    }
}
