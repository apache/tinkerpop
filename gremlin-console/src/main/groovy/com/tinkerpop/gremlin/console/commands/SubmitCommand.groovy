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
    def Object execute(final List<String> arguments) {
        if (mediator.remotes.size() == 0) return "No remotes are configured.  Use :remote command."
        return mediator.currentRemote().submit(arguments)
    }
}