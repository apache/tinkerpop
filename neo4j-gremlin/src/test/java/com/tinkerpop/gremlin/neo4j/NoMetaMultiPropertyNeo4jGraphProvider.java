package com.tinkerpop.gremlin.neo4j;

import com.tinkerpop.gremlin.TestHelper;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class NoMetaMultiPropertyNeo4jGraphProvider extends AbstractNeo4jGraphProvider {
    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName) {
        return new HashMap<String, Object>() {{
            put("gremlin.graph", Neo4jGraph.class.getName());
            put(Neo4jGraph.CONFIG_DIRECTORY, getWorkingDirectory() + File.separator + TestHelper.cleanPathSegment(graphName) + File.separator + TestHelper.cleanPathSegment(testMethodName));
            put(Neo4jGraph.CONFIG_META_PROPERTIES, false);
            put(Neo4jGraph.CONFIG_MULTI_PROPERTIES, false);
            put(Neo4jGraph.CONFIG_CHECK_ELEMENTS_IN_TRANSACTION, true);
        }};
    }
}
