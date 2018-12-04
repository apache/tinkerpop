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
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.function.Function;

/**
 * Generalized serializer for {#code Enum} value types.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class EnumSerializer<E extends Enum> extends SimpleTypeSerializer<E> {

    public static final EnumSerializer<SackFunctions.Barrier> BarrierSerializer = new EnumSerializer<>(DataType.BARRIER, SackFunctions.Barrier::valueOf);
    public static final EnumSerializer<VertexProperty.Cardinality> CardinalitySerializer = new EnumSerializer<>(DataType.CARDINALITY, VertexProperty.Cardinality::valueOf);
    public static final EnumSerializer<Column> ColumnSerializer = new EnumSerializer<>(DataType.COLUMN, Column::valueOf);
    public static final EnumSerializer<Direction> DirectionSerializer = new EnumSerializer<>(DataType.DIRECTION, Direction::valueOf);
    public static final EnumSerializer<Operator> OperatorSerializer = new EnumSerializer<>(DataType.OPERATOR, Operator::valueOf);
    public static final EnumSerializer<Order> OrderSerializer = new EnumSerializer<>(DataType.ORDER, Order::valueOf);
    public static final EnumSerializer<TraversalOptionParent.Pick> PickSerializer = new EnumSerializer<>(DataType.PICK, TraversalOptionParent.Pick::valueOf);
    public static final EnumSerializer<Pop> PopSerializer = new EnumSerializer<>(DataType.POP, Pop::valueOf);
    public static final EnumSerializer<Scope> ScopeSerializer = new EnumSerializer<>(DataType.SCOPE, Scope::valueOf);
    public static final EnumSerializer<T> TSerializer = new EnumSerializer<>(DataType.T, T::valueOf);

    private final Function<String, E> readFunc;

    private EnumSerializer(final DataType dataType, final Function<String, E> readFunc) {
        super(dataType);
        this.readFunc = readFunc;
    }

    @Override
    E readValue(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        return readFunc.apply(context.read(buffer));
    }

    @Override
    public ByteBuf writeValue(final E value, final ByteBufAllocator allocator, final GraphBinaryWriter context) throws SerializationException {
        return context.write(value.name(), allocator);
    }
}
