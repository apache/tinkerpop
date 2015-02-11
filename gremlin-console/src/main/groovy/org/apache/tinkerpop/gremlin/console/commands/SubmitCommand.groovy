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