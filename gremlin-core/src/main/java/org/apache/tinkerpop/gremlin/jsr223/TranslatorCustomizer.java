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
package org.apache.tinkerpop.gremlin.jsr223;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;

/**
 * Provides a way to customize and override {@link Bytecode} to script translation. Not all {@link GremlinScriptEngine}
 * will support this capability as translation is optional.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface TranslatorCustomizer extends Customizer {

    /**
     * Construct a {@link Translator.ScriptTranslator.TypeTranslator} that will be used by a
     * {@link Translator.ScriptTranslator} instance within the {@link GremlinScriptEngine} to translate
     * {@link Bytecode} to a script.
     */
    public Translator.ScriptTranslator.TypeTranslator createTypeTranslator();
}
