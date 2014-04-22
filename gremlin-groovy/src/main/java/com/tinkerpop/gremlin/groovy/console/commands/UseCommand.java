package com.tinkerpop.gremlin.groovy.console.commands;

import groovy.grape.Grape;
import org.codehaus.groovy.tools.shell.CommandSupport;
import org.codehaus.groovy.tools.shell.Groovysh;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UseCommand extends CommandSupport {

    public UseCommand(Groovysh shell) {
        super(shell, ":use", ":u");
    }

    public Object execute(final List<String> arguments) {
        final String group = arguments.get(0);
        final String module = arguments.get(1);
        final String version = arguments.get(2);

        if (group == null || group.isEmpty())
            throw new IllegalArgumentException("group cannot be null or empty");

        if (module == null || module.isEmpty())
            throw new IllegalArgumentException("module cannot be null or empty");

        if (version == null || version.isEmpty())
            throw new IllegalArgumentException("version cannot be null or empty");

        final Map<String, Object> map = new HashMap<>();
        map.put("classLoader", shell.getInterp().getClassLoader());
        map.put("group", group);
        map.put("module", module);
        map.put("version", version);
        map.put("changing", false);
        Grape.grab(map);
        return "loaded: " + map;

        // TODO: Stephen, I have no idea what this is all about.
        // note that the service loader utilized the classloader from the groovy shell as shell class are available
        // from within there given loading through Grape.
        /*ServiceLoader.load(ConsolePlugin.class, Console.groovysh.interp.classLoader).each {
            if (!Gremlin.plugins().contains(it.name)) {
                it.pluginTo(new ConsoleGroovy(Console.groovysh), new ConsoleIO(it, Console.standardIo), dependency.args);
                Gremlin.plugins().add(it.name)
                Console.standardIo.out.println(Console.STANDARD_RESULT_PROMPT + "Plugin Loaded: " + it.name)
            }
        }*/
    }
}
