package com.tinkerpop.gremlin.groovy.console.commands;

import com.tinkerpop.gremlin.driver.Item;
import com.tinkerpop.gremlin.groovy.console.Mediator;
import org.codehaus.groovy.tools.shell.CommandSupport;
import org.codehaus.groovy.tools.shell.Groovysh;

import java.util.List;

/**
 * Submit a script to a Gremlin Server instance.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SubmitCommand extends CommandSupport {

    private final Mediator mediator;

    public SubmitCommand(final Groovysh shell, final Mediator mediator) {
        super(shell, ":submit", ":>");
        this.mediator = mediator;
    }

    @Override
    public Object execute(final List<String> arguments) {
        final String line = String.join(" ", arguments);

        try {
            final List<Item> resultSet = mediator.submit(line);
            shell.getInterp().getContext().setProperty("_l", resultSet);
            return resultSet;
        } catch (Exception ex) {
            return ex.getMessage();
        }
    }
}