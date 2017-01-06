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
package org.apache.tinkerpop.gremlin.groovy.jsr223;

import org.apache.tinkerpop.gremlin.groovy.DefaultImportCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.ConfigurationCustomizerProvider;
import org.junit.Test;

import javax.script.ScriptEngine;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinGroovyScriptEngineConfigTest {
    @Test
    public void shouldAddBaseScriptClassDeprecated() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine(
                new ConfigurationCustomizerProvider("ScriptBaseClass", BaseScriptForTesting.class.getName()),
                new DefaultImportCustomizerProvider());

        assertEquals("hello, stephen", engine.eval("hello('stephen')"));
    }

    @Test
    public void shouldAddBaseScriptClass() throws Exception {
        final ScriptEngine engine = new GremlinGroovyScriptEngine(
                new ConfigurationGroovyCustomizer("ScriptBaseClass", BaseScriptForTesting.class.getName()));

        assertEquals("hello, stephen", engine.eval("hello('stephen')"));
    }
}
