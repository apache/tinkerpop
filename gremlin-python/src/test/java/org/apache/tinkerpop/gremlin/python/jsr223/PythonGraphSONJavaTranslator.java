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

package org.apache.tinkerpop.gremlin.python.jsr223;

import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.jsr223.ScriptEngineCache;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeHelper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV2d0;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV3d0;
import org.apache.tinkerpop.shaded.jackson.core.JsonFactory;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class PythonGraphSONJavaTranslator<S extends TraversalSource, T extends Traversal.Admin<?, ?>> implements Translator.StepTranslator<S, T> {

    private final boolean IS_TESTING = Boolean.valueOf(System.getProperty("is.testing", "false"));
    private final PythonTranslator pythonTranslator;
    private final JavaTranslator<S, T> javaTranslator;
    private final GraphSONReader reader = GraphSONReader.build().mapper(
            GraphSONMapper.build().addCustomModule(GraphSONXModuleV2d0.build().create(false))
                    .version(GraphSONVersion.V2_0).create()).create();
    private final GraphSONWriter writer = GraphSONWriter.build().mapper(
            GraphSONMapper.build().addCustomModule(GraphSONXModuleV2d0.build().create(false))
                    .version(GraphSONVersion.V2_0).create()).create();

    public PythonGraphSONJavaTranslator(final PythonTranslator pythonTranslator, final JavaTranslator<S, T> javaTranslator) {
        this.pythonTranslator = pythonTranslator;
        this.javaTranslator = javaTranslator;
    }

    @Override
    public S getTraversalSource() {
        return this.javaTranslator.getTraversalSource();
    }

    @Override
    public String getTargetLanguage() {
        return this.javaTranslator.getTargetLanguage();
    }

    @Override
    public T translate(final Bytecode bytecode) {
        try {
            final ScriptEngine jythonEngine = ScriptEngineCache.get("jython");
            final Bindings bindings = jythonEngine.createBindings();
            bindings.putAll(jythonEngine.getBindings(ScriptContext.ENGINE_SCOPE));
            bindings.put(this.pythonTranslator.getTraversalSource(), jythonEngine.eval("Graph().traversal()"));
            bindings.putAll(bytecode.getBindings());
            final String translatedGraphSONBytecode = jythonEngine.eval("graphson_writer.writeObject(" + this.pythonTranslator.translate(bytecode) + ")", bindings).toString();
            if (IS_TESTING) {
                // verify that the GraphSON sent to Python is the same as the GraphSON returned by Python
                final ByteArrayOutputStream output = new ByteArrayOutputStream();
                BytecodeHelper.removeBindings(bytecode); // this is because bindings are variables that get converted to values at translation
                BytecodeHelper.detachElements(bytecode); // this is to get the minimal necessary representation
                this.writer.writeObject(output, bytecode);
                final String originalGraphSONBytecode = new String(output.toByteArray());
                final ObjectMapper mapper = new ObjectMapper(new JsonFactory());
                final Map<String, Object> original = mapper.readValue(originalGraphSONBytecode, Map.class);
                final Map<String, Object> translated = mapper.readValue(translatedGraphSONBytecode, Map.class);
                assertEquals(originalGraphSONBytecode.length(), translatedGraphSONBytecode.length());
                assertEquals(original, translated);
            }
            return this.javaTranslator.translate(this.reader.readObject(new ByteArrayInputStream(translatedGraphSONBytecode.getBytes()), Bytecode.class));


        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }

    }
}
