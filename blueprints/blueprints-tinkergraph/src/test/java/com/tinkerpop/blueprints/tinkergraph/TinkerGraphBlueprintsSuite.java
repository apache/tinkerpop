package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.BlueprintsSuite;
import org.junit.runner.RunWith;

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
}
