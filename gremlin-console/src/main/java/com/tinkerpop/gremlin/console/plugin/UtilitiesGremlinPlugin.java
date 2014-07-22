package com.tinkerpop.gremlin.console.plugin;

import com.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.codehaus.groovy.tools.shell.IO;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class UtilitiesGremlinPlugin extends AbstractGremlinPlugin {

    @Override
    public String getName() {
        return "utilities";
    }

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) {
        super.pluginTo(pluginAcceptor);

        String line;
        try {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("UtilitiesGremlinPluginScript.groovy")));
            while ((line = reader.readLine()) != null) {
                pluginAcceptor.eval(line);
            }
            reader.close();
        } catch (Exception ex) {
            final IO io = (IO) pluginAcceptor.environment().get(ConsolePluginAcceptor.ENVIRONMENT_IO);
            io.out.println("Error loading the 'utilities' plugin - " + ex.getMessage());
        }
    }
}
