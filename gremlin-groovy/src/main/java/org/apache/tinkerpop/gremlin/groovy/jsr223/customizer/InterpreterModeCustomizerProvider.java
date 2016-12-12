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
import org.apache.tinkerpop.gremlin.groovy.jsr223.ast.InterpreterMode;
import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.codehaus.groovy.control.customizers.ASTTransformationCustomizer;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;

/**
 * Places the {@code ScriptEngine} in "interpreter mode" where local variables of a script are treated as global
 * bindings. This implementation is technically not a true {@link CompilerCustomizerProvider} instance as the
 * "interpreter mode" feature does not require a {@code CompilerCustomizer}. This class merely acts as a flag that
 * tells the {@link org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine} to turn this feature on.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.2.4, not replaced by a public class.
 */
@Deprecated
public class InterpreterModeCustomizerProvider implements CompilerCustomizerProvider {
    @Override
    public CompilationCustomizer create() {
        return new ASTTransformationCustomizer(InterpreterMode.class);
    }
}
