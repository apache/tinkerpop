package com.tinkerpop.gremlin.groovy.console.commands

import com.tinkerpop.gremlin.groovy.console.DriverRemoteAcceptor
import com.tinkerpop.gremlin.groovy.console.Mediator
import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor
import org.codehaus.groovy.tools.shell.ComplexCommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

/**
 * Configure a remote connection to a Gremlin Server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class RemoteCommand extends ComplexCommandSupport {
    private final Mediator mediator

    public RemoteCommand(final Groovysh shell, final Mediator mediator) {
        super(shell, ":remote", ":rem", ["current", "connect", "list", "next", "prev"], "current")
        this.mediator = mediator
    }

    // todo: add a select option
    // todo: add a way to name remotes

    /*
    def Object do_timeout = { List<String> arguments ->
        final String errorMessage = "the timeout option expects a positive integer representing milliseconds or 'max' as an argument"
        if (arguments.size() != 1) return errorMessage
        try {
            final int to = arguments.get(0).equals("max") ? Integer.MAX_VALUE : Integer.parseInt(arguments.get(0))
            if (to <= 0) return errorMessage

            mediator.remoteTimeout = to
            return "set remote timeout to ${to}ms"
        } catch (Exception ex) {
            return errorMessage
        }
    }
    */

    def Object do_connect = { List<String> arguments ->
        if (arguments.size() == 0) return "define the remote to configured (e.g. server)"

        def remote
        if (arguments[0] == "server") {
            // assume a remote gremlin server
            remote = new DriverRemoteAcceptor(shell)
        } else {
            if (!mediator.loadedPlugins.containsKey(arguments[0])) return "no plugin named ${arguments[0]}"
            def plugin = mediator.loadedPlugins[arguments[0]]
            def Optional<RemoteAcceptor> remoteAcceptor = plugin.remoteAcceptor()
            if (!remoteAcceptor.isPresent()) return "${arguments[0]} does not accept remote configuration"

            remote = remoteAcceptor.get()
        }

        mediator.addRemote(remote)

        return remote.connect(arguments.tail())
    }

    def Object do_current = {
        return "remote - ${mediator.currentRemote()}"
    }

    def Object do_next = {
        mediator.nextRemote()
    }

    def Object do_prev = {
        mediator.previousRemote()
    }

    def Object do_list = {
        def copy = []
        mediator.remotes.eachWithIndex{remote, i -> copy << (mediator.position == i ? "*" : "") + i + " - " + remote.toString()}
        return copy
    }
}
