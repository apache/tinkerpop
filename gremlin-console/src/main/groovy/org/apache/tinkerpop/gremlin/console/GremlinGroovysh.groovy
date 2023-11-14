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
            if (mediator.localEvaluation)
                return super.execute(line)
            else {
                assert line != null
                if (line.trim().size() == 0) {
                    return null
                }

                maybeRecordInput(line)

                Object result

                if (isExecutable(line)) {
                    result = executeCommand(line)
                    if (result != null) setLastResult(result)
                    return result
                }

                List<String> current = new ArrayList<String>(buffers.current())
                current << line

                String importsSpec = this.getImportStatements()

                // determine if this script is complete or not - if not it's a multiline script
                def status = parser.parse([importsSpec] + current)

                switch (status.code) {
                    case ParseCode.COMPLETE:
                        if (!Boolean.valueOf(getPreference(INTERPRETER_MODE_PREFERENCE_KEY, 'false')) || isTypeOrMethodDeclaration(current)) {
                            // Evaluate the current buffer w/imports and dummy statement
                            List buff = [importsSpec] + [ 'true' ] + current
                            try {
                                interp.evaluate(buff)
                            } catch(MultipleCompilationErrorsException t) {
                                if (isIncompleteCaseOfAntlr4(t)) {
                                    // treat like INCOMPLETE case
                                    buffers.updateSelected(current)
                                    break
                                }
                                throw t
                            } catch (MissingPropertyException mpe) {
                                // Ignore any local missing properties since it doesn't affect remote execution.
                            }
                        } else {
                            // Evaluate Buffer wrapped with code storing bounded vars
                            try {
                                evaluateWithStoredBoundVars(importsSpec, current)
                            } catch(MultipleCompilationErrorsException t) {
                                if (isIncompleteCaseOfAntlr4(t)) {
                                    // treat like INCOMPLETE case
                                    buffers.updateSelected(current)
                                    break
                                }
                                throw t
                            } catch (MissingPropertyException mpe) {
                                // Ignore any local missing properties since it doesn't affect remote execution.
                            }
                        }

                        // concat script here because commands don't support multi-line
                        def script = String.join(Parser.getNEWLINE(), current)
                        setLastResult(mediator.currentRemote().submit([script]))
                        buffers.clearSelected()
                        break
                    case ParseCode.INCOMPLETE:
                        buffers.updateSelected(current)
                        break
                    case ParseCode.ERROR:
                        throw status.cause
                    default:
                        // Should never happen
                        throw new Error("Invalid parse status: $status.code")
                }

                return result
            }
        } finally {
            mediator.evaluating.set(false)
        }
    }

    private void setLastResult(final Object result) {
        if (resultHook == null) {
            throw new IllegalStateException('Result hook is not set')
        }

        resultHook.call((Object)result)

        interp.context['_'] = result

        maybeRecordResult(result)
    }

    private Object evaluateWithStoredBoundVars(String importsSpec, List<String> current) {
        Object result
        String variableBlocks = null
        // To make groovysh behave more like an interpreter, we need to retrieve all bound
        // vars at the end of script execution, and then update them into the groovysh Binding context.
        Set<String> boundVars = ScriptVariableAnalyzer.getBoundVars(importsSpec + Parser.NEWLINE + current.join(Parser.NEWLINE), interp.classLoader)
        if (boundVars) {
            variableBlocks = "$COLLECTED_BOUND_VARS_MAP_VARNAME = new HashMap();"
            boundVars.each({ String varname ->
                // bound vars can be in global or some local scope.
                // We discard locally scoped vars by ignoring MissingPropertyException
                variableBlocks += """
try {$COLLECTED_BOUND_VARS_MAP_VARNAME[\"$varname\"] = $varname;
} catch (MissingPropertyException e){}"""
            })
        }
        // Evaluate the current buffer w/imports and dummy statement
        List<String> buff
        if (variableBlocks) {
            buff = [importsSpec] + ['try {', 'true'] + current + ['} finally {' + variableBlocks + '}']
        } else {
            buff = [importsSpec] + ['true'] + current
        }
        interp.evaluate(buff)

        if (variableBlocks) {
            def boundVarValues = (Map<String, Object>) interp.context.getVariable(COLLECTED_BOUND_VARS_MAP_VARNAME)
            boundVarValues.each({ String name, Object value -> interp.context.setVariable(name, value) })
        }

        return result
    }

    private boolean isIncompleteCaseOfAntlr4(MultipleCompilationErrorsException t) {
        // TODO antlr4 parser errors pop out here - can we rework to be like antlr2?
        (
            (t.message.contains('Unexpected input: ') || t.message.contains('Unexpected character: ')) && !(
                t.message.contains("Unexpected input: '}'")
                    || t.message.contains("Unexpected input: ')'")
                    || t.message.contains("Unexpected input: ']'")
            )
        )
    }
}
