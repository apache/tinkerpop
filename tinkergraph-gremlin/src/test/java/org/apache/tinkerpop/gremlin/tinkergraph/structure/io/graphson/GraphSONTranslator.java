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

package org.apache.tinkerpop.gremlin.tinkergraph.structure.io.graphson;

import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV2d0;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV3d0;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class GraphSONTranslator<S extends TraversalSource, T extends Traversal.Admin<?, ?>> implements Translator.StepTranslator<S, T> {

    private final JavaTranslator<S, T> wrappedTranslator;
    private final GraphSONWriter writer;
    private final GraphSONReader reader;

    public GraphSONTranslator(final JavaTranslator<S, T> wrappedTranslator) {
        this(wrappedTranslator, GraphSONVersion.V2_0);
    }

    public GraphSONTranslator(final JavaTranslator<S, T> wrappedTranslator, final GraphSONVersion version) {
        this.wrappedTranslator = wrappedTranslator;
        final GraphSONMapper mapper;
        if (version == GraphSONVersion.V2_0) {
            mapper = GraphSONMapper.build()
                    .addCustomModule(GraphSONXModuleV2d0.build().create(false)).version(GraphSONVersion.V2_0).create();
        } else if (version == GraphSONVersion.V3_0) {
            mapper = GraphSONMapper.build()
                    .addCustomModule(GraphSONXModuleV3d0.build().create(false)).version(GraphSONVersion.V3_0).create();
        } else {
            throw new IllegalArgumentException("GraphSONVersion." + version.name() + " is not supported for testing");
        }

        writer = GraphSONWriter.build().mapper(mapper).create();
        reader = GraphSONReader.build().mapper(mapper).create();
    }

    @Override
    public S getTraversalSource() {
        return this.wrappedTranslator.getTraversalSource();
    }

    @Override
    public T translate(final Bytecode bytecode) {
        try {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            this.writer.writeObject(outputStream, bytecode);
            return this.wrappedTranslator.translate(this.reader.readObject(new ByteArrayInputStream(outputStream.toByteArray()), Bytecode.class));
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public String getTargetLanguage() {
        return this.wrappedTranslator.getTargetLanguage();
    }
}
