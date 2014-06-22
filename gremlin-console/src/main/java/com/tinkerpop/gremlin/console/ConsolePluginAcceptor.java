package com.tinkerpop.gremlin.console;

import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.codehaus.groovy.tools.shell.Groovysh;

import javax.script.ScriptException;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ConsolePluginAcceptor implements PluginAcceptor {

    private final Groovysh shell;

    public ConsolePluginAcceptor(final Groovysh shell) {
        this.shell = shell;
    }

    @Override
    public void addImports(final Set<String> importStatements) {
        importStatements.forEach(this.shell::execute);
    }

    @Override
    public Object eval(final String script) throws ScriptException {
        return this.shell.execute(script);
    }
}
