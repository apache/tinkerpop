package com.tinkerpop.gremlin.console.plugin;

import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import com.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;
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
    public void afterPluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {
        try {
            pluginAcceptor.eval(SugarLoader.class.getPackage().getName() + "." + SugarLoader.class.getSimpleName());
            pluginAcceptor.eval(SugarLoader.class.getSimpleName() + ".load()");
        } catch (Exception ex) {
            if (io != null)
                io.out.println("Error loading the 'tinkerpop.sugar' plugin - " + ex.getMessage());
            else
                throw new PluginInitializationException("Error loading the 'tinkerpop.sugar' plugin - " + ex.getMessage(), ex);
        }
    }
}
