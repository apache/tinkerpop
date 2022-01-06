/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.console.commands

import org.apache.tinkerpop.gremlin.console.Mediator
import org.apache.tinkerpop.gremlin.jsr223.console.RemoteAcceptor
import org.apache.tinkerpop.gremlin.jsr223.console.RemoteException
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
        super(shell, ":remote", ":rem", ["current", "connect", "config", "list", "next", "prev", "choose", "close", "console"], "current")
        this.mediator = mediator
    }

    def Object do_connect = { List<String> arguments ->
        if (arguments.size() == 0) return "Define the remote to configured (e.g. tinkerpop.server)"

        if (!mediator.availablePlugins.values().any {
            it.plugin.name == arguments[0]
        }) return "No plugin named ${arguments[0]}"

        def pluggedIn = mediator.availablePlugins.values().find { it.plugin.name == arguments[0] }
        if (!pluggedIn.activated) return "Plugin is available but not activated with ':plugin use ${arguments[0]}'"

        def Optional<RemoteAcceptor> remoteAcceptor = pluggedIn.remoteAcceptor()
        if (!remoteAcceptor.isPresent()) return "${arguments[0]} does not accept remote configuration"

        def remote = remoteAcceptor.get()
        try {
            def result = remote.connect(arguments.tail())
            mediator.addRemote(remote)
            return result
        } catch (RemoteException re) {
            remote.close()
            return re.message
        }
    }

    def Object do_config = { List<String> arguments ->
        if (mediator.remotes.size() == 0) return "Please add a remote first with [connect]"
        try {
            return mediator.currentRemote().configure(arguments)
        } catch (RemoteException re) {
            return re.message
        }
    }

    def Object do_current = {
        if (mediator.remotes.size() == 0) return "Please add a remote first with [connect]"
        return "Remote - ${mediator.currentRemote()}"
    }

    def Object do_choose = { List<String> arguments ->
        if (mediator.remotes.size() == 0) return "Please add a remote first with [connect]"
        if (arguments.size() != 1) return "Specify the numeric index of the remote"

        def pos
        try {
            pos = Integer.parseInt(arguments.first())
        } catch (Exception ex) {
            return "Index must be an integer value"
        }

        if (pos >= mediator.remotes.size() || pos < 0) return "Index is out of range - use [list] to see indices available"

        mediator.position = pos
        return mediator.currentRemote()
    }

    def Object do_next = {
        if (mediator.remotes.size() == 0) return "Please add a remote first with [connect]"
        mediator.nextRemote()
    }

    def Object do_prev = {
        if (mediator.remotes.size() == 0) return "Please add a remote first with [connect]"
        mediator.previousRemote()
    }

    def Object do_list = {
        def copy = []
        mediator.remotes.eachWithIndex { remote, i -> copy << (mediator.position == i ? "*" : "") + i + " - " + remote.toString() }
        return copy
    }

    def Object do_close = {
        if (mediator.remotes.size() == 0) return "Please add a remote first with [connect]"

        // the console is in remote evaluation mode.  closing at this point will needs to exit that mode and then
        // kill the remote itself
        if (!mediator.localEvaluation) swapEvaluationMode()

        def removed = mediator.removeCurrent()
        removed.close()
        return "Removed - $removed"
    }

    def Object do_console = {
        if (mediator.remotes.size() == 0) return "Please add a remote first with [connect]"
        if (!mediator.currentRemote().allowRemoteConsole()) return "The ${mediator.currentRemote()} is not compatible with 'connect' - use ':>' instead"
        return swapEvaluationMode()
    }

    private String swapEvaluationMode() {
        mediator.localEvaluation = !mediator.localEvaluation
        if (mediator.localEvaluation)
            return "All scripts will now be evaluated locally - type ':remote console' to return to remote mode for ${mediator.currentRemote()}"
        else
            return "All scripts will now be sent to ${mediator.currentRemote()} - type ':remote console' to return to local mode"
    }
}
