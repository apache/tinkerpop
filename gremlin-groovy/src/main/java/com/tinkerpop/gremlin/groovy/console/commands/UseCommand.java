package com.tinkerpop.gremlin.groovy.console.commands;

import com.tinkerpop.gremlin.groovy.console.ConsolePluginAcceptor;
import com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import groovy.grape.Grape;
import org.codehaus.groovy.tools.shell.CommandSupport;
import org.codehaus.groovy.tools.shell.Groovysh;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UseCommand extends CommandSupport {

    private final Set<String> loadedPlugins;

    public UseCommand(final Groovysh shell, final Set<String> loadedPlugins) {
        super(shell, ":use", ":u");
        this.loadedPlugins = loadedPlugins;
    }

    public Object execute(final List<String> arguments) {
        final String group = arguments.get(0);
        final String module = arguments.get(1);
        final String version = arguments.get(2);

        if (group == null || group.isEmpty())
            throw new IllegalArgumentException("Group cannot be null or empty");

        if (module == null || module.isEmpty())
            throw new IllegalArgumentException("Module cannot be null or empty");

        if (version == null || version.isEmpty())
            throw new IllegalArgumentException("Version cannot be null or empty");

        final Map<String, Object> map = new HashMap<>();
        map.put("classLoader", shell.getInterp().getClassLoader());
        map.put("group", group);
        map.put("module", module);
        map.put("version", version);
        map.put("changing", false);
        Grape.grab(map);

        // note that the service loader utilized the classloader from the groovy shell as shell class are available
        // from within there given loading through Grape.
        ServiceLoader.load(GremlinPlugin.class, shell.getInterp().getClassLoader()).forEach(plugin -> {
            if (!loadedPlugins.contains(plugin.getName())) {
                plugin.pluginTo(new ConsolePluginAcceptor(shell));
                loadedPlugins.add(plugin.getName());
            }
        });

        return "loaded: " + map;
    }
}
