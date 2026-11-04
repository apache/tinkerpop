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
package org.apache.tinkerpop.gremlin.console.commands

import org.jline.reader.Candidate
import org.jline.reader.ParsedLine
import org.jline.reader.Parser
import org.jline.reader.impl.DefaultParser
import org.jline.reader.impl.completer.ArgumentCompleter
import org.jline.reader.impl.completer.NullCompleter
import org.jline.shell.Command

class CommandCompletion {

    /**
     * Completes the last word of {@code line} the way groovysh does for a command's arguments.
     */
    static List<String> complete(final Command command, final String line) {
        final ParsedLine parsed = new DefaultParser().parse(line, line.length(), Parser.ParseContext.COMPLETE)
        final List<Candidate> candidates = []
        new ArgumentCompleter(command.completers() + [NullCompleter.INSTANCE]).complete(null, parsed, candidates)
        return candidates.collect { it.value() }.findAll { it.startsWith(parsed.word()) }.sort()
    }
}
