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
package org.apache.tinkerpop.gremlin.console

import groovy.transform.ThreadInterrupt
import org.apache.groovy.groovysh.Command
import org.apache.groovy.groovysh.Groovysh
import org.apache.groovy.groovysh.ParseCode
import org.apache.groovy.groovysh.Parser
import org.apache.groovy.groovysh.util.CommandArgumentParser
import org.apache.groovy.groovysh.util.ScriptVariableAnalyzer
import org.apache.tinkerpop.gremlin.console.commands.GremlinSetCommand
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.MultipleCompilationErrorsException
import org.codehaus.groovy.control.customizers.ASTTransformationCustomizer
import org.codehaus.groovy.tools.shell.IO

/**
 * Overrides the posix style parsing of Groovysh allowing for commands to parse prior to Groovy 2.4.x.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinGroovysh extends Groovysh {

    private final Mediator mediator
    private final static CompilerConfiguration compilerConfig = new CompilerConfiguration(CompilerConfiguration.DEFAULT) {{
        addCompilationCustomizers(new ASTTransformationCustomizer(ThreadInterrupt.class))
    }}

    public GremlinGroovysh(final Mediator mediator, final IO io) {
        super(io, compilerConfig)
        this.mediator = mediator
    }

    public CompilerConfiguration getCompilerConfiguration() { return compilerConfig }

    protected List parseLine(final String line) {
        assert line != null
        return line.trim().tokenize()
    }

    @Override
    Command findCommand(final String line, final List<String> parsedArgs = null) {
        def l = line ?: ""

        final List<String> linetokens = parseLine(l)
        if (linetokens.size() == 0) return null

        def cmd = registry.find(linetokens[0])

        if (cmd != null && linetokens.size() > 1 && parsedArgs != null) {
            if (cmd instanceof GremlinSetCommand) {
                // the following line doesn't play well with :remote scripts because it tokenizes quoted args as single args
                // but at this point only the set command had trouble with quoted params/args with spaces
                List<String> args = CommandArgumentParser.parseLine(line, parsedArgs == null ? 1 : -1)
                parsedArgs.addAll(args[1..-1])
            } else {
                parsedArgs.addAll(linetokens[1..-1])
            }
        }

        return cmd
    }

    @Override
    Object execute(final String line) {
        mediator.evaluating.set(true)

        try {
            return super.execute(line)
        } finally {
            mediator.evaluating.set(false)
        }
    }
}
