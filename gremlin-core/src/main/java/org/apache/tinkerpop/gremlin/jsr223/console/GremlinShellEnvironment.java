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
package org.apache.tinkerpop.gremlin.jsr223.console;

/**
 * Provides an abstraction over a "Gremlin Shell" (i.e. the core of a console), enabling the plugin to not have to
 * be hardcoded specifically to any particular shell, like the Gremlin Groovy Console, and thus allowing it to
 * not have to depend on the gremlin-groovy module itself.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface GremlinShellEnvironment {

    public <T> T getVariable(final String variableName);

    public <T> void setVariable(final String variableName, final T variableValue);

    public void println(final String line);

    public default void errPrintln(final String line) {
        println(line);
    }

    public <T> T execute(final String line);
}
