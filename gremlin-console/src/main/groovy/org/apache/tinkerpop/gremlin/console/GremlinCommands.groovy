/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.console

import org.apache.groovy.groovysh.jline.GroovyEngine
import org.apache.tinkerpop.gremlin.console.commands.ClsCommand
import org.apache.tinkerpop.gremlin.console.commands.GremlinSetCommand
import org.apache.tinkerpop.gremlin.console.commands.InstallCommand
import org.apache.tinkerpop.gremlin.console.commands.PluginCommand
import org.apache.tinkerpop.gremlin.console.commands.UninstallCommand
import org.jline.shell.Command
import org.jline.shell.CommandGroup
import org.jline.shell.impl.SimpleCommandGroup

/**
 * The commands the Gremlin Console adds to groovysh. groovysh names commands {@code /name} and also accepts a typed
 * {@code :name} for any command or alias that exists.
 */
class GremlinCommands {

    static CommandGroup create(final Mediator mediator, final GroovyEngine engine) {
        return new SimpleCommandGroup("gremlin", [
                new PluginCommand(mediator),
                new GremlinSetCommand(),
                new InstallCommand(mediator, engine),
                new UninstallCommand(mediator),
                new ClsCommand(mediator)] as Command[])
    }
}
