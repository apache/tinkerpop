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
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper
import org.codehaus.groovy.tools.shell.ComplexCommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

/**
 * Commands that help work with Gremlin bytecode.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class BytecodeCommand extends ComplexCommandSupport {

    private final Mediator mediator

    private final ObjectMapper mapper = GraphSONMapper.build().version(GraphSONVersion.V3_0).create().createMapper()

    public BytecodeCommand(final Groovysh shell, final Mediator mediator) {
        super(shell, ":bytecode", ":bc", ["eval", "from", "submit", "translate"])
        this.mediator = mediator
    }
    
    def Object do_from = { List<String> arguments ->
        mediator.showShellEvaluationOutput(false)
        def args = arguments.join("")
        def traversal = args.startsWith("@") ? shell.interp.context.getVariable(args.substring(1)) : shell.execute(args)
        if (!(traversal instanceof Traversal))
            return "Argument does not resolve to a Traversal instance, was: " + traversal.class.simpleName

        mediator.showShellEvaluationOutput(true)
        return mapper.writeValueAsString(traversal.asAdmin().bytecode)
    }
    
    def Object do_eval = { List<String> arguments ->
        return shell.execute(do_translate(arguments))
    }

    def Object do_submit = { List<String> arguments ->
        if (mediator.remotes.size() == 0) return "No remotes are configured.  Use :remote command."
        return mediator.currentRemote().submit([do_translate(arguments)])
    }


    def Object do_translate = { List<String> arguments ->
        def g = arguments[0]
        def args = arguments.drop(1).join("")
        def graphson = args.startsWith("@") ? shell.interp.context.getVariable(args.substring(1)) : args
        return GroovyTranslator.of(g).translate(mapper.readValue(graphson, Bytecode.class))
    }
}
