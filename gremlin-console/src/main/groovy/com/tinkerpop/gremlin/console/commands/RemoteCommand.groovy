package com.tinkerpop.gremlin.console.commands

import com.tinkerpop.gremlin.console.Mediator
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
        super(shell, ":remote", ":rem", ["current", "connect", "config", "list", "next", "prev", "choose", "close"], "current")
        this.mediator = mediator
    }

    def Object do_connect = { List<String> arguments ->
        if (arguments.size() == 0) return "define the remote to configured (e.g. server)"

        if (!mediator.availablePlugins.values().any{it.plugin.name==arguments[0]}) return "no plugin named ${arguments[0]}"
        def plugin = mediator.availablePlugins.values().find{it.plugin.name==arguments[0]}.plugin
        def Optional<RemoteAcceptor> remoteAcceptor = plugin.remoteAcceptor()
        if (!remoteAcceptor.isPresent()) return "${arguments[0]} does not accept remote configuration"

        def remote = remoteAcceptor.get()

        mediator.addRemote(remote)

        return remote.connect(arguments.tail())
    }

    def Object do_config = { List<String> arguments ->
        if (mediator.remotes.size() == 0) return "please add a remote first with [connect]"
        return mediator.currentRemote().configure(arguments)
    }

    def Object do_current = {
        if (mediator.remotes.size() == 0) return "please add a remote first with [connect]"
        return "remote - ${mediator.currentRemote()}"
    }

    def Object do_choose = { List<String> arguments ->
        if (mediator.remotes.size() == 0) return "please add a remote first with [connect]"
        if (arguments.size() != 1) return "specify the numeric index of the remote"

        def pos
        try {
            pos = Integer.parseInt(arguments.first())
        } catch (Exception ex) {
            return "index must be an integer value"
        }

        if (pos >= mediator.remotes.size() || pos < 0) return "index is out of range - use [list] to see indices available"

        mediator.position = pos
        return mediator.currentRemote()
    }

    def Object do_next = {
        if (mediator.remotes.size() == 0) return "please add a remote first with [connect]"
        mediator.nextRemote()
    }

    def Object do_prev = {
        if (mediator.remotes.size() == 0) return "please add a remote first with [connect]"
        mediator.previousRemote()
    }

    def Object do_list = {
        def copy = []
        mediator.remotes.eachWithIndex { remote, i -> copy << (mediator.position == i ? "*" : "") + i + " - " + remote.toString() }
        return copy
    }

    def Object do_close = {
        def removed = mediator.removeCurrent()
        removed.close()
        return "removed - $removed"
    }
}
