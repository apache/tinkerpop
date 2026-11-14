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

import org.codehaus.groovy.control.CompilationUnit
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.MultipleCompilationErrorsException
import org.codehaus.groovy.control.Phases
import org.jline.reader.EOFError
import org.jline.reader.ParsedLine
import org.jline.reader.Parser
import org.jline.reader.impl.DefaultParser

import java.util.function.Predicate

/**
 * Decides when typed, piped or scripted input is a complete statement. The line reader's own parser only waits for
 * more input while a bracket, quote or comment is open, so a line such as {@code g.V().} or {@code x = 1 +} would run
 * at once and fail. This parser also waits when the statement only fails to compile because it ends too early.
 */
class StatementParser implements Parser {
    private final Parser delegate
    private final CompilerConfiguration config
    private final Predicate<String> isCommand

    /**
     * @param delegate the line reader's parser
     * @param config the compiler settings of the engine the statement runs in
     * @param isCommand whether a name such as {@code /plugin} is a console command
     */
    StatementParser(final Parser delegate, final CompilerConfiguration config, final Predicate<String> isCommand) {
        this.delegate = delegate
        this.config = config
        this.isCommand = isCommand
    }

    /**
     * Creates a parser for script files, with the line reader settings groovysh uses for typed input.
     */
    static StatementParser forScripts(final CompilerConfiguration config) {
        final DefaultParser lineParser = new DefaultParser(regexCommand: /\/?[a-zA-Z!]\S*/, eofOnUnclosedQuote: true,
                eofOnEscapedNewLine: true)
        lineParser.blockCommentDelims(new DefaultParser.BlockCommentDelims("/*", "*/"))
                .lineCommentDelims(["//"] as String[])
                .setEofOnUnclosedBracket(DefaultParser.Bracket.CURLY, DefaultParser.Bracket.ROUND, DefaultParser.Bracket.SQUARE)
        // console commands in scripts are handled before a statement is parsed
        return new StatementParser(lineParser, config, { false } as Predicate<String>)
    }

    @Override
    ParsedLine parse(final String line, final int cursor, final ParseContext context) {
        if (context != ParseContext.ACCEPT_LINE) return delegate.parse(line, cursor, context)

        final boolean command = isCommandLine(line)
        try {
            final ParsedLine parsed = delegate.parse(line, cursor, context)
            if (!command && continuesOnNextLine(line)) throw new StatementContinues()
            return parsed
        } catch (EOFError e) {
            // more input cannot fix brackets that do not match, and a command with an unclosed quote is reported by
            // the command, so both are accepted as they are
            if (e instanceof StatementContinues || !(command || cannotBeClosed(line, e))) throw e
            return delegate.parse(line, cursor, ParseContext.SPLIT_LINE)
        }
    }

    /**
     * Whether the text is an unfinished statement that the next line continues.
     */
    boolean isUnfinished(final String text) {
        try {
            parse(text, text.length(), ParseContext.ACCEPT_LINE)
            return false
        } catch (EOFError ignored) {
            return true
        }
    }

    private boolean isCommandLine(final String line) {
        final String trimmed = line.trim()
        if (!trimmed.startsWith(":") && !trimmed.startsWith("/")) return false
        return isCommand.test(GremlinCommands.slashName(trimmed.split(/\s+/, 2)[0]))
    }

    /**
     * Whether the statement fails to parse only because it ends too early, e.g. after a trailing {@code .} or an
     * operator: it does not parse as it is but does once a name is added after it. The position of Groovy's error
     * cannot be used instead, as {@code repeat(out()).} is reported at the call rather than at the end.
     */
    boolean continuesOnNextLine(final String statement) {
        final String code = statement.stripTrailing()
        // ":" starts no Groovy statement, so an unknown command is a mistake rather than an unfinished line
        if (code.isEmpty() || code.startsWith(":")) return false

        return !parses(code) && parses(code + "\n" + NEXT_LINE)
    }

    /**
     * Stands in for whatever the next line holds.
     */
    private static final String NEXT_LINE = "_"

    private boolean parses(final String code) {
        // parse only, so no class is generated or loaded
        final CompilationUnit unit = new CompilationUnit(config)
        unit.addSource("Script1.groovy", code)
        try {
            unit.compile(Phases.CONVERSION)
            return true
        } catch (MultipleCompilationErrorsException ignored) {
            return false
        }
    }

    /**
     * Whether more input cannot fix the brackets, e.g. {@code [1, 2))}, which the line reader reports as missing a
     * {@code ]}.
     */
    private boolean cannotBeClosed(final String statement, final EOFError e) {
        if (e.missing?.startsWith("missing: ")) return true
        if (!e.missing?.startsWith("add: ")) return false
        final String closing = e.missing.substring("add: ".length())
        return !parses(statement + "\n" + closing) && !parses(statement + "\n" + NEXT_LINE + "\n" + closing)
    }

    /**
     * The statement continues because it ends in an operator or similar, rather than in an open bracket or quote.
     */
    private static class StatementContinues extends EOFError {
        StatementContinues() {
            super(-1, -1, "Statement continues", "continue")
        }
    }

    @Override
    boolean isEscapeChar(final char ch) { delegate.isEscapeChar(ch) }

    @Override
    boolean validCommandName(final String name) { delegate.validCommandName(name) }

    @Override
    boolean validVariableName(final String name) { delegate.validVariableName(name) }

    @Override
    String getCommand(final String line) { delegate.getCommand(line) }

    @Override
    String getVariable(final String line) { delegate.getVariable(line) }
}
