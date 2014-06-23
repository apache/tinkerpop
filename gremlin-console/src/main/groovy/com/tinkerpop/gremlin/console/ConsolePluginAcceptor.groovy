package com.tinkerpop.gremlin.console

import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor
import org.codehaus.groovy.tools.shell.Groovysh

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class ConsolePluginAcceptor implements PluginAcceptor {

    private final Groovysh shell;

    public ConsolePluginAcceptor(final Groovysh shell)  { this.shell = shell }

    @Override
    public void addImports(final Set<String> importStatements) {
        importStatements.each { this.shell.execute(it) }
    }

    @Override
    public Object eval(final String script) throws javax.script.ScriptException { return this.shell.execute(script) }
}