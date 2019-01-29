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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;

import java.util.List;

public class ByteCodeSerializer extends SimpleTypeSerializer<Bytecode> {
    public ByteCodeSerializer() {
        super(DataType.BYTECODE);
    }

    @Override
    protected Bytecode readValue(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        final Bytecode result = new Bytecode();

        final int stepsLength = buffer.readInt();
        for (int i = 0; i < stepsLength; i++) {
            result.addStep(context.readValue(buffer, String.class, false), getInstructionArguments(buffer, context));
        }

        final int sourcesLength = buffer.readInt();
        for (int i = 0; i < sourcesLength; i++) {
            result.addSource(context.readValue(buffer, String.class, false), getInstructionArguments(buffer, context));
        }

        return result;
    }

    private static Object[] getInstructionArguments(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        final int valuesLength = buffer.readInt();
        final Object[] values = new Object[valuesLength];
        for (int j = 0; j < valuesLength; j++) {
            values[j] = context.read(buffer);
        }
        return values;
    }

    @Override
    protected ByteBuf writeValue(final Bytecode value, final ByteBufAllocator allocator, final GraphBinaryWriter context) throws SerializationException {
        final List<Bytecode.Instruction> steps = value.getStepInstructions();
        final List<Bytecode.Instruction> sources = value.getSourceInstructions();
        // 2 buffers for the length + plus 2 buffers per each step and source
        final CompositeByteBuf result = allocator.compositeBuffer(2 + steps.size() * 2 + sources.size() * 2);

        writeInstructions(allocator, context, steps, result);
        writeInstructions(allocator, context, sources, result);

        return result;
    }

    private void writeInstructions(final ByteBufAllocator allocator, final GraphBinaryWriter context,
                                   final List<Bytecode.Instruction> instructions, final CompositeByteBuf result) throws SerializationException {

        result.addComponent(true, context.writeValue(instructions.size(), allocator, false));

        for (Bytecode.Instruction instruction : instructions) {
            result.addComponents(
                    true,
                    context.writeValue(instruction.getOperator(), allocator, false),
                    getArgumentsBuffer(instruction.getArguments(), allocator, context));
        }
    }

    private static ByteBuf getArgumentsBuffer(final Object[] arguments, final ByteBufAllocator allocator, final GraphBinaryWriter context) throws SerializationException {
        final CompositeByteBuf result = allocator.compositeBuffer(1 + arguments.length);
        result.addComponent(true, context.writeValue(arguments.length, allocator, false));

        for (Object value : arguments) {
            result.addComponent(true, context.write(value, allocator));
        }

        return result;
    }
}
