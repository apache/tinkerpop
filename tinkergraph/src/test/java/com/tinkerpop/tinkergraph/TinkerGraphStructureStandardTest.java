package com.tinkerpop.tinkergraph;

import com.tinkerpop.gremlin.structure.StructureStandardSuite;
import com.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.HashMap;
import java.util.Map;


/**
 * Executes the Simple Blueprints Test Suite using TinkerGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(StructureStandardSuite.class)
@StructureStandardSuite.GraphProviderClass(TinkerGraphStructureStandardTest.class)
public class TinkerGraphStructureStandardTest extends StructureStandardSuite.AbstractGraphProvider {

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName) {
        // todo: when tinkergraph has persistence this will need to change to ensure unique graphs are generated...now it's all in memory
        return new HashMap<String, Object>() {{
            put("gremlin.graph", TinkerGraph.class.getName());
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        g.close();

        if (configuration.containsKey("gremlin.tg.directory")) {
            // this is a non-in-memory configuration so blow away the directory
            final File graphDirectory = new File(configuration.getString("gremlin.tg.directory"));
            graphDirectory.delete();
        }
    }
}
