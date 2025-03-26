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

import org.apache.tinkerpop.gremlin.process.traversal.DT;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;

import java.io.IOException;
import java.util.function.Function;

/**
 * Generalized serializer for {#code Enum} value types.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class EnumSerializer<E extends Enum> extends SimpleTypeSerializer<E> {

    public static final EnumSerializer<Direction> DirectionSerializer = new EnumSerializer<>(DataType.DIRECTION, Direction::valueOf);
    public static final EnumSerializer<T> TSerializer = new EnumSerializer<>(DataType.T, T::valueOf);
    public static final EnumSerializer<Merge> MergeSerializer = new EnumSerializer<>(DataType.MERGE, Merge::valueOf);

    private final Function<String, E> readFunc;

    private EnumSerializer(final DataType dataType, final Function<String, E> readFunc) {
        super(dataType);
        this.readFunc = readFunc;
    }

    @Override
    protected E readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        return readFunc.apply(context.read(buffer));
    }

    @Override
    protected void writeValue(final E value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        context.write(value.name(), buffer);
    }
}
