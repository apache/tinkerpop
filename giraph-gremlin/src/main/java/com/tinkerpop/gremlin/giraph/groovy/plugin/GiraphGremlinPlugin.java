package com.tinkerpop.gremlin.giraph.groovy.plugin;


import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGremlinPlugin { //implements GremlinPlugin {

    private static final String IMPORT = "import ";
    private static final String DOT_STAR = ".*";

    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT + GiraphGraph.class.getPackage().getName() + DOT_STAR);
    }};

    //@Override
    public String getName() {
        return "giraph";
    }

    /*@Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) {
        pluginAcceptor.addImports(IMPORTS);
    }*/
}