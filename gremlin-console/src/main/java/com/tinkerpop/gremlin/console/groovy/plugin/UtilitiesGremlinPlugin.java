package com.tinkerpop.gremlin.console.groovy.plugin;

import com.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import com.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;
import groovyx.gbench.Benchmark;
import groovyx.gbench.BenchmarkStaticExtension;
import groovyx.gprof.ProfileStaticExtension;
import groovyx.gprof.Profiler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class UtilitiesGremlinPlugin extends AbstractGremlinPlugin {

    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT_SPACE + Benchmark.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_STATIC_SPACE + BenchmarkStaticExtension.class.getName() + DOT_STAR);
        add(IMPORT_SPACE + Profiler.class.getPackage().getName() + DOT_STAR);
        add(IMPORT_STATIC_SPACE + ProfileStaticExtension.class.getName() + DOT_STAR);
    }};

    @Override
    public String getName() {
        return "tinkerpop.utilities";
    }

    @Override
    public void afterPluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {
        pluginAcceptor.addImports(IMPORTS);

        String line;
        try {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("UtilitiesGremlinPluginScript.groovy")));
            while ((line = reader.readLine()) != null) {
                pluginAcceptor.eval(line);
            }
            reader.close();
        } catch (Exception ex) {
            if (io != null)
                io.out.println("Error loading the 'utilities' plugin - " + ex.getMessage());
            else
                throw new PluginInitializationException("Error loading the 'utilities' plugin - " + ex.getMessage());
        }
    }
}
