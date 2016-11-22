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
package org.apache.tinkerpop.gremlin.console.jsr223;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.console.ConsoleCustomizer;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;

import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GephiGremlinPlugin extends AbstractGremlinPlugin {
    private static final String NAME = "tinkerpop.gephi";

    public GephiGremlinPlugin() {
        super(NAME, new GephiConsoleCustomizer());
    }

    private static class GephiConsoleCustomizer implements ConsoleCustomizer {
        @Override
        public org.apache.tinkerpop.gremlin.jsr223.console.RemoteAcceptor getRemoteAcceptor(final Map<String, Object> environment) {
            return new GephiRemoteAcceptor((Groovysh) environment.get(ConsoleCustomizer.ENV_CONSOLE_SHELL),
                    (IO) environment.get(ConsoleCustomizer.ENV_CONSOLE_IO));
        }
    }
}
