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
package org.apache.tinkerpop.gremlin.groovy.jsr223.customizer;

import org.apache.tinkerpop.gremlin.groovy.CompilerCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;

/**
 * Provides compilation configuration options to the {@link GremlinGroovyScriptEngine}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.2.5, not replaced by a public class.
 */
@Deprecated
public class CompilationOptionsCustomizerProvider implements CompilerCustomizerProvider {

    private final int expectedCompilationTime;

    public CompilationOptionsCustomizerProvider(final int expectedCompilationTime) {
        this.expectedCompilationTime = expectedCompilationTime;
    }

    public int getExpectedCompilationTime() {
        return expectedCompilationTime;
    }

    @Override
    public CompilationCustomizer create() {
        throw new UnsupportedOperationException("This is a marker implementation that does not create a CompilationCustomizer instance");
    }
}
