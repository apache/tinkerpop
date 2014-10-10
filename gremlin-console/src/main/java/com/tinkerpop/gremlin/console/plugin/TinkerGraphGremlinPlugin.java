package com.tinkerpop.gremlin.console.plugin;

import com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import com.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TinkerGraphGremlinPlugin implements GremlinPlugin {

    private static final String IMPORT = "import ";
    private static final String DOT_STAR = ".*";

    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT + TinkerGraph.class.getPackage().getName() + DOT_STAR);
        add(IMPORT + TinkerGraphComputer.class.getPackage().getName() + DOT_STAR);
    }};

    @Override
    public String getName() {
        return "tinkerpop.tinkergraph";
    }

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) {
        pluginAcceptor.addImports(IMPORTS);
    }
}
