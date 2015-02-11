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
package com.apache.tinkerpop.gremlin.console.commands

import com.apache.tinkerpop.gremlin.console.Mediator
import com.apache.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor
import com.apache.tinkerpop.gremlin.groovy.plugin.RemoteException
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
        if (arguments.size() == 0) return "Define the remote to configured (e.g. tinkerpop.server)"

        if (!mediator.availablePlugins.values().any {
            it.plugin.name == arguments[0]
        }) return "No plugin named ${arguments[0]}"

        def pluggedIn = mediator.availablePlugins.values().find { it.plugin.name == arguments[0] }
        if (!pluggedIn.activated) return "Plugin is available but not activated with ':plugin use ${arguments[0]}'"

        def plugin = pluggedIn.plugin
        def Optional<RemoteAcceptor> remoteAcceptor = plugin.remoteAcceptor()
        if (!remoteAcceptor.isPresent()) return "${arguments[0]} does not accept remote configuration"

        try {
            def remote = remoteAcceptor.get()
            def result = remote.connect(arguments.tail())
            mediator.addRemote(remote)
            return result
        } catch (RemoteException re) {
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
        def removed = mediator.removeCurrent()
        removed.close()
        return "Removed - $removed"
    }
}
