package com.tinkerpop.tinkergraph.process;

import com.tinkerpop.gremlin.AbstractGraphProvider;
import com.tinkerpop.gremlin.process.ProcessComputerStandardSuite;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.tinkergraph.structure.TinkerGraph;
import org.apache.commons.configuration.Configuration;
import org.junit.Ignore;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Executes the Standard Gremlin Process Test Suite using TinkerGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(ProcessComputerStandardSuite.class)
@Ignore
@ProcessComputerStandardSuite.GraphProviderClass(TinkerGraphProcessComputerStandardTest.class)
public class TinkerGraphProcessComputerStandardTest extends AbstractGraphProvider {

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName) {
        // todo: when tinkergraph has persistence this will need to change to ensure unique graphs are generated...now it's all in memory
        return new HashMap<String, Object>() {{
            put("gremlin.graph", TinkerGraph.class.getName());
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (g != null)
            g.close();

        if (configuration.containsKey("gremlin.tg.directory")) {
            // this is a non-in-memory configuration so blow away the directory
            final File graphDirectory = new File(configuration.getString("gremlin.tg.directory"));
            graphDirectory.delete();
        }
    }
}
