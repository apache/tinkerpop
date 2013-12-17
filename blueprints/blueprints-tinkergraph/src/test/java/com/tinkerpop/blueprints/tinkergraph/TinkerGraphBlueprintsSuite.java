package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.BlueprintsSuite;
import com.tinkerpop.blueprints.Graph;
import org.apache.commons.configuration.Configuration;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.HashMap;
import java.util.Map;


/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(BlueprintsSuite.class)
@BlueprintsSuite.GraphProviderClass(TinkerGraphBlueprintsSuite.class)
public class TinkerGraphBlueprintsSuite extends BlueprintsSuite.AbstractGraphProvider {
    @Override
    public Map<String, Object> getBaseConfiguration() {
        return new HashMap<String, Object>() {{
            put("blueprints.graph", TinkerGraph.class.getName());
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        g.close();

        if (configuration.containsKey("blueprints.tg.directory")) {
            // this is a non-in-memory configuration so blow away the directory
            final File graphDirectory = new File(configuration.getString("blueprints.tg.directory"));
            graphDirectory.delete();
        }
    }
}
