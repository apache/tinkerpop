package com.tinkerpop.gremlin.console.plugin

import com.tinkerpop.gremlin.groovy.plugin.GremlinPlugin
import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor
import org.codehaus.groovy.tools.shell.Groovysh
import org.codehaus.groovy.tools.shell.IO

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class ConsolePluginAcceptor implements PluginAcceptor {

    public static final String ENVIRONMENT_NAME = "console";
    public static final String ENVIRONMENT_SHELL = "ConsolePluginAcceptor.shell"
    public static final String ENVIRONMENT_IO = "ConsolePluginAcceptor.io"

    private final Groovysh shell
    private final IO io

    public ConsolePluginAcceptor(final Groovysh shell, final IO io) {
        this.shell = shell
        this.io = io
    }

    @Override
    void addBinding(final String key, final Object val) {
        shell.interp.context.setVariable(key, val)
    }

    @Override
    Map<String, Object> getBindings() {
        return Collections.unmodifiableMap(shell.interp.context.variables)
    }

    @Override
    void addImports(final Set<String> importStatements) {
        importStatements.each { this.shell.execute(it) }
    }

    @Override
    Object eval(final String script) throws javax.script.ScriptException { return this.shell.execute(script) }

    @Override
    Map<String, Object> environment() {
        return [(GremlinPlugin.ENVIRONMENT): ENVIRONMENT_NAME, (ENVIRONMENT_IO): io, (ENVIRONMENT_SHELL): shell]
    }
}