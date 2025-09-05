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
package org.apache.tinkerpop.gremlin.structure.io.binary.types;

import org.apache.tinkerpop.gremlin.structure.GType;
import org.apache.tinkerpop.gremlin.structure.GremlinDataType;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GremlinDataTypeSerializer extends SimpleTypeSerializer<GremlinDataType> {
    // Registry of all known TypeToken implementations
    private static final Map<String, TypeTokenFactory> TYPE_FACTORIES = new HashMap<>();

    static {
        // Register your enum types here
        registerTypeFactory("GType", GType::valueOf);
        // Add more as needed
    }

    public GremlinDataTypeSerializer() {
        super(DataType.GREMLINDATATYPE);
    }

    /**
     * Register a factory for creating TypeToken instances from strings
     */
    public static void registerTypeFactory(String typeName, TypeTokenFactory factory) {
        TYPE_FACTORIES.put(typeName, factory);
    }

    @Override
    protected GremlinDataType readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        // Read the enum class name
        final String enumClassName = context.read(buffer);
        // Read the enum value name
        final String enumValueName = context.read(buffer);

        System.out.println("Reading " + enumClassName + " with value " + enumValueName);

        TypeTokenFactory factory = TYPE_FACTORIES.get(enumClassName);
        if (factory == null) {
            throw new IOException("Unknown TypeToken enum class: " + enumClassName);
        }

        try {
            return factory.create(enumValueName);
        } catch (Exception e) {
            throw new IOException("Failed to create TypeToken for " + enumClassName + "." + enumValueName, e);
        }
    }

    @Override
    protected void writeValue(final GremlinDataType value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        // Write the enum class name (simple name)
        String enumClassName = value.getClass().getSimpleName();
        context.write(enumClassName, buffer);

        System.out.println("Writing " + enumClassName + " to " + buffer);

        // Write the enum value name
        if (value instanceof Enum) {
            context.write(((Enum<?>) value).name(), buffer);
        } else {
            // Fallback for non-enum implementations
            context.write(value.getType(), buffer);
        }
    }

    @FunctionalInterface
    public interface TypeTokenFactory {
        GremlinDataType create(String name) throws Exception;
    }

}
