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

import org.codehaus.groovy.tools.shell.Command
import org.codehaus.groovy.tools.shell.Groovysh

/**
 * Overrides the posix style parsing of Groovysh allowing for commands to parse prior to Groovy 2.4.x.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinGroovysh extends Groovysh {
    protected List parseLine(final String line) {
        assert line != null
        return line.trim().tokenize()
    }

    @Override
    Command findCommand(final String line, final List<String> parsedArgs = null) {
        def l = line ?: ""

        final List<String> args = parseLine(l)
        if (args.size() == 0) return null

        def cmd = registry.find(args[0])

        if (cmd != null && args.size() > 1 && parsedArgs != null) {
            parsedArgs.addAll(args[1..-1])
        }

        return cmd
    }
}
