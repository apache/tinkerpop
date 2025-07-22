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

import org.apache.groovy.groovysh.ParseCode
import org.apache.groovy.groovysh.ParseStatus
import org.apache.groovy.groovysh.Parsing
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.MultipleCompilationErrorsException
import org.codehaus.groovy.control.messages.Message
import org.codehaus.groovy.control.messages.SyntaxErrorMessage

/**
 * A {@link Parsing} implementation that detects if a Groovy script is complete or incomplete. This implementation uses
 * the Groovy compiler to try to compile the script and analyzes any compilation errors to determine if the script is
 * incomplete. It does not evaluate the script, which prevents local execution and potential local failure.
 */
class LocalSafeParser implements Parsing {

    // Try to compile the script
    private final CompilerConfiguration config = new CompilerConfiguration()

    // Create a custom GroovyClassLoader that doesn't write class files
    private final GroovyClassLoader gcl = new GroovyClassLoader(this.class.classLoader, config) {
        @Override
        public Class defineClass(String name, byte[] bytes) {
            // Skip writing to disk, just define the class in memory
            return super.defineClass(name, bytes, 0, bytes.length)
        }
    }
    /**
     * Parse the given buffer and determine if it's a complete Groovy script.
     *
     * @param buffer The list of strings representing the script lines
     * @return A ParseStatus indicating if the script is complete, incomplete, or has errors
     */
    @Override
    ParseStatus parse(Collection<String> buffer) {
        if (!buffer) {
            return new ParseStatus(ParseCode.COMPLETE)
        }

        // join the buffer lines into a single script
        def script = buffer.join('\n')

        try {
            // Parse the script without generating .class files
            gcl.parseClass(script)
            // If we get here, the script compiled successfully
            return new ParseStatus(ParseCode.COMPLETE)
        } catch (MultipleCompilationErrorsException e) {
            // Check if the errors indicate an incomplete script
            if (isIncompleteScript(e)) {
                return new ParseStatus(ParseCode.INCOMPLETE)
            } else {
                // Other compilation errors
                return new ParseStatus(ParseCode.ERROR, e)
            }
        } catch (Exception e) {
            // Any other exception is considered an error
            return new ParseStatus(ParseCode.ERROR, e)
        }
    }

    /**
     * Determine if the compilation errors indicate an incomplete script.
     *
     * @param e The compilation errors exception
     * @return true if the script is incomplete, false otherwise
     */
    private boolean isIncompleteScript(MultipleCompilationErrorsException e) {
        def errorCollector = e.errorCollector

        for (Message message : errorCollector.errors) {
            if (message instanceof SyntaxErrorMessage) {
                def syntaxException = message.cause
                def errorMessage = syntaxException.message

                // Check for common indicators of incomplete scripts
                if (isUnexpectedEOF(errorMessage) ||
                        isUnclosedBracket(errorMessage) ||
                        isUnclosedString(errorMessage) ||
                        isUnexpectedInput(errorMessage)) {
                    return true
                }
            }
        }

        return false
    }

    /**
     * Check if the error message indicates an unexpected end of file.
     */
    private boolean isUnexpectedEOF(String errorMessage) {
        errorMessage.contains('unexpected end of file') ||
                errorMessage.contains('Unexpected EOF')
    }

    /**
     * Check if the error message indicates an unclosed bracket.
     */
    private boolean isUnclosedBracket(String errorMessage) {
        errorMessage.contains("Missing '}'") ||
                errorMessage.contains("Missing ')'") ||
                errorMessage.contains("Missing ']'")
    }

    /**
     * Check if the error message indicates an unclosed string.
     */
    private boolean isUnclosedString(String errorMessage) {
        errorMessage.contains('String literal is not terminated') ||
                errorMessage.contains('Unterminated string literal')
    }

    /**
     * Check if the error message indicates unexpected input that might suggest
     * an incomplete script rather than a syntax error.
     */
    private boolean isUnexpectedInput(String errorMessage) {
        // Check for unexpected input but exclude cases where closing brackets are unexpected
        // as those typically indicate syntax errors rather than incomplete scripts
        (errorMessage.contains('Unexpected input: ') ||
                errorMessage.contains('Unexpected character: ')) &&
                !(errorMessage.contains("Unexpected input: '}'") ||
                        errorMessage.contains("Unexpected input: ')'") ||
                        errorMessage.contains("Unexpected input: ']'"))
    }
}
