/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.groovy.jsr223;

import org.apache.tinkerpop.gremlin.jsr223.CoreImports;
import org.apache.tinkerpop.gremlin.jsr223.ScriptEngineCache;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GroovyScriptEngineSetup {

    private GroovyScriptEngineSetup() {
    }

    public static void setup() {
        setup(ScriptEngineCache.get("groovy"));
    }

    public static ScriptEngine setup(final ScriptEngine groovyScriptEngine) {
        try {
            for (Class<?> c : CoreImports.getClassImports()) {
                groovyScriptEngine.eval("import " + c.getName());
            }

            final Set<Class<?>> staticImports = new HashSet<>();

            for (Enum e : CoreImports.getEnumImports()) {
                staticImports.add(e.getDeclaringClass());
            }

            for (Method m : CoreImports.getMethodImports()) {
                staticImports.add(m.getDeclaringClass());
            }

            for (Class<?> c : staticImports) {
                groovyScriptEngine.eval("import static " + c.getName() + ".*");
            }

            return groovyScriptEngine;
        } catch (final ScriptException ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }
}
