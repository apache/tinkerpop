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
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.translator.GroovyTranslator
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry
import org.apache.tinkerpop.gremlin.structure.io.Mapper
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV3d0
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule
import org.codehaus.groovy.tools.shell.ComplexCommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

/**
 * Commands that help work with Gremlin bytecode.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class BytecodeCommand extends ComplexCommandSupport {

    private final Mediator mediator

    private ObjectMapper mapper

    public BytecodeCommand(final Groovysh shell, final Mediator mediator) {
        super(shell, ":bytecode", ":bc", ["config", "from", "reset", "translate"])
        this.mediator = mediator
        do_reset()
    }

    def Object do_config = { List<String> arguments ->
        def resolvedArgs = arguments.collect{shell.interp.context.getVariable(it)}
        try {
            this.initMapper(resolvedArgs)
            return "Configured bytecode serializer"
        } catch (IllegalArgumentException iae) {
            return iae.message
        }
    }
    
    def Object do_from = { List<String> arguments ->
        mediator.showShellEvaluationOutput(false)
        def args = arguments.join(" ")
        def traversal = args.startsWith("@") ? shell.interp.context.getVariable(args.substring(1)) : shell.execute(args)
        if (!(traversal instanceof Traversal))
            return "Argument does not resolve to a Traversal instance, was: " + traversal.class.simpleName

        mediator.showShellEvaluationOutput(true)
        return mapper.writeValueAsString(traversal.asAdmin().bytecode)
    }

    def Object do_translate = { List<String> arguments ->
        def g = arguments[0]
        def args = arguments.drop(1).join(" ")
        def graphson = args.startsWith("@") ? shell.interp.context.getVariable(args.substring(1)) : args
        return GroovyTranslator.of(g).translate(mapper.readValue(graphson, Bytecode.class))
    }

    def Object do_reset = { List<String> arguments ->
        def (GraphSONMapper.Builder builder, boolean loadedTinkerGraph) = createDefaultBuilder()

        try {
            this.initMapper([builder.create()])
            return "Bytecode serializer reset to GraphSON 3.0 with extensions" +
                    (loadedTinkerGraph ? " and TinkerGraph serializers" : "")
        } catch (IllegalArgumentException iae) {
            return iae.message
        }
    }

    private def static createDefaultBuilder() {
        def builder = GraphSONMapper.build().
                addCustomModule(GraphSONXModuleV3d0.build().create(false)).
                version(GraphSONVersion.V3_0)

        def loadedTinkerGraph = false
        try {
            def tinkergraphIoRegistry = Class.forName("org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0")
            builder.addRegistry(tinkergraphIoRegistry."instance"())
            loadedTinkerGraph = true
        } catch (Exception ignored) {
            // ok to skip if the registry isn't present
        }
        [builder, loadedTinkerGraph]
    }

    private initMapper(def args) {
        if (args.size() == 1 && args[0] instanceof GraphSONMapper) {
            this.mapper = ((GraphSONMapper) args[0]).createMapper()
        } else {
            GraphSONMapper.Builder builder = (GraphSONMapper.Builder) createDefaultBuilder()[0]
            args.each {
                if (it instanceof GraphSONMapper)
                    throw new IllegalArgumentException("If specifying a GraphSONMapper it must be the only argument")
                else if (it instanceof IoRegistry)
                    builder.addRegistry(it)
                else if (it instanceof SimpleModule)
                    builder.addCustomModule(it)
                else
                    throw new IllegalArgumentException("Configuration argument of ${it.class.simpleName} is ignored - must be IoRegistry, SimpleModule or a single GraphSONMapper")
            }

            this.mapper = builder.create().createMapper()
        }
    }
}
