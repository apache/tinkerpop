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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;

public interface TypeSerializer<T> {
    /**
     * Reads the type information and value from the buffer and returns an instance of T.
     */
    T read(ByteBuf buffer, GraphBinaryReader context) throws SerializationException;

    /**
     * Reads the value from the buffer (not the type information) and returns an instance of T.
     * <p>
     *     Implementors should throw an exception when a complex type doesn't support reading without the type
     *     information
     * </p>
     */
    T readValue(ByteBuf buffer, GraphBinaryReader context, boolean nullable) throws SerializationException;

    /**
     * Writes the type code, information and value to buffer.
     */
    ByteBuf write(T value, ByteBufAllocator allocator, GraphBinaryWriter context) throws SerializationException;

    /**
     * Writes the value to buffer, composed by the value flag and the sequence of bytes.
     */
    ByteBuf writeValue(T value, ByteBufAllocator allocator, GraphBinaryWriter context, boolean nullable)throws SerializationException;
}
