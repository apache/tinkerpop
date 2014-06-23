package com.tinkerpop.gremlin.console.commands

import com.tinkerpop.gremlin.console.Mediator
import org.codehaus.groovy.tools.shell.CommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

/**
 * Submit a script to a Gremlin Server instance.
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class SubmitCommand extends CommandSupport {

    private final Mediator mediator

    public SubmitCommand(final Groovysh shell, final Mediator mediator) {
        super(shell, ":submit", ":>")
        this.mediator = mediator
    }

    @Override
    def Object execute(final List<String> arguments) { return mediator.currentRemote().submit(arguments) }
}