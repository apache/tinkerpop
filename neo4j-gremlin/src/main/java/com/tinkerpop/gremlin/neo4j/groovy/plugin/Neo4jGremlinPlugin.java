package com.tinkerpop.gremlin.neo4j.groovy.plugin;

import com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import com.tinkerpop.gremlin.neo4j.groovy.loaders.Neo4jStepLoader;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jGremlinPlugin implements GremlinPlugin {

    private static final String IMPORT = "import ";
    private static final String DOT_STAR = ".*";

    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT + Neo4jGraph.class.getPackage().getName() + DOT_STAR);
    }};

    @Override
    public String getName() {
        return "neo4j";
    }

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) {
        pluginAcceptor.addImports(IMPORTS);
        try {
            pluginAcceptor.eval(Neo4jStepLoader.class.getCanonicalName() + ".load()");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
