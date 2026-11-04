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

import org.jline.reader.Candidate
import org.jline.reader.Completer
import org.jline.reader.CompletingParsedLine
import org.jline.reader.LineReader
import org.jline.reader.ParsedLine

import java.util.function.Predicate

/**
 * Completes the arguments of a command typed as {@code :name}. groovysh runs {@code :name} as {@code /name} but
 * only completes arguments for {@code /name}, so this presents the line to the wrapped completer in that form.
 */
class ColonCommandCompleter implements Completer {
    private final Completer delegate
    private final Predicate<String> isCommand

    ColonCommandCompleter(final Completer delegate, final Predicate<String> isCommand) {
        this.delegate = delegate
        this.isCommand = isCommand
    }

    @Override
    void complete(final LineReader reader, final ParsedLine line, final List<Candidate> candidates) {
        delegate.complete(reader, asSlashCommand(line), candidates)
    }

    ParsedLine asSlashCommand(final ParsedLine line) {
        if (line.wordIndex() < 1 || !(line instanceof CompletingParsedLine)) return line
        final String first = line.words()[0]
        if (!first.startsWith(":") || !isCommand.test("/" + first.substring(1))) return line
        return new SlashLine((CompletingParsedLine) line, ["/" + first.substring(1)] + line.words().drop(1))
    }

    private static class SlashLine implements CompletingParsedLine {
        private final CompletingParsedLine line
        private final List<String> words

        SlashLine(final CompletingParsedLine line, final List<String> words) {
            this.line = line
            this.words = words
        }

        @Override
        List<String> words() { words }

        @Override
        String word() { line.word() }

        @Override
        int wordCursor() { line.wordCursor() }

        @Override
        int wordIndex() { line.wordIndex() }

        @Override
        String line() { line.line() }

        @Override
        int cursor() { line.cursor() }

        @Override
        CharSequence escape(final CharSequence candidate, final boolean complete) { line.escape(candidate, complete) }

        @Override
        int rawWordCursor() { line.rawWordCursor() }

        @Override
        int rawWordLength() { line.rawWordLength() }
    }
}
