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
package org.apache.tinkerpop.gremlin.driver.ser.binary.types;

import io.netty.buffer.ByteBuf;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;

public class ByteCodeSerializer extends SimpleTypeSerializer<Bytecode> {

    @Override
    public Bytecode readValue(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
        final Bytecode result = new Bytecode();

        final int stepsLength = buffer.readInt();
        for (int i = 0; i < stepsLength; i++) {
            result.addStep(context.readValue(buffer, String.class), getValues(buffer, context));
        }

        final int sourcesLength = buffer.readInt();
        for (int i = 0; i < sourcesLength; i++) {
            result.addSource(context.readValue(buffer, String.class), getValues(buffer, context));
        }

        return result;
    }

    private static Object[] getValues(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
        final int valuesLength = buffer.readInt();
        Object[] values = new Object[valuesLength];
        for (int j = 0; j < valuesLength; j++) {
            values[j] = context.readFullyQualifiedObject(buffer);
        }
        return values;
    }
}
