package com.tinkerpop.gremlin.console.plugin;

import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.codehaus.groovy.tools.shell.IO;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SugarGremlinPlugin extends AbstractGremlinPlugin {

    @Override
    public String getName() {
        return "tinkerpop.sugar";
    }

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) {
        super.pluginTo(pluginAcceptor);
        try {
            pluginAcceptor.eval(SugarLoader.class.getPackage().getName() + "." + SugarLoader.class.getSimpleName());
            pluginAcceptor.eval(SugarLoader.class.getSimpleName() + ".load()");
        } catch (Exception ex) {
            final IO io = (IO) pluginAcceptor.environment().get(ConsolePluginAcceptor.ENVIRONMENT_IO);
            io.out.println("Error loading the 'tinkerpop.sugar' plugin - " + ex.getMessage());
        }
    }
}
