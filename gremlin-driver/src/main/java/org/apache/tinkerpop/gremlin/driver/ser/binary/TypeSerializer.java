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
package org.apache.tinkerpop.gremlin.driver.ser.binary;

import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;

/**
 * Represents a serializer for a given type.
 */
public interface TypeSerializer<T> {

    /**
     * Gets the {@link DataType} that is represented by the given {@link T}.
     */
    DataType getDataType();

    /**
     * Reads the type information and value from the buffer and returns an instance of T.
     */
    T read(final Buffer buffer, final GraphBinaryReader context) throws SerializationException;

    /**
     * Reads the value from the buffer (not the type information) and returns an instance of T.
     * <p>
     *     Implementors should throw an exception when a complex type doesn't support reading without the type
     *     information.
     * </p>
     */
    T readValue(final Buffer buffer, final GraphBinaryReader context, final boolean nullable) throws SerializationException;

    /**
     * Writes the type code, information and value to a buffer using the provided allocator.
     */
    void write(final T value, final Buffer buffer, final GraphBinaryWriter context) throws SerializationException;

    /**
     * Writes the value to a buffer, composed by the value flag and the sequence of bytes.
     */
    void writeValue(final T value, final Buffer buffer, final GraphBinaryWriter context, final boolean nullable) throws SerializationException;
}
